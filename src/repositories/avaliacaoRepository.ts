import { DataSource, Repository, Between } from "typeorm";
import { DateTime } from "luxon";
import {
  AvaliacaoSCP,
  ClassificacaoCuidado,
  StatusSessaoAvaliacao,
} from "../entities/AvaliacaoSCP";
import { UnidadeInternacao } from "../entities/UnidadeInternacao";
import { Leito, StatusLeito } from "../entities/Leito";
import {
  CreateAvaliacaoSCPDTO,
  FiltroAvaliacaoDTO,
  ResumoDiarioDTO,
} from "../dto/avaliacao.dto";
import { classificarTotal } from "../utils/scpFaixas";
import { ScpMetodo } from "../entities/ScpMetodo";
import { Colaborador } from "../entities/Colaborador";
import { LeitosStatusService } from "../services/leitosStatusService";

export class AvaliacaoRepository {
  private repo: Repository<AvaliacaoSCP>;
  private unidadeRepo: Repository<UnidadeInternacao>;
  private scpMetodoRepo: Repository<ScpMetodo>;
  private colaboradorRepo: Repository<Colaborador>;
  private leitosStatusService: LeitosStatusService;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(AvaliacaoSCP);
    this.unidadeRepo = ds.getRepository(UnidadeInternacao);
    this.scpMetodoRepo = ds.getRepository(ScpMetodo);
    this.colaboradorRepo = ds.getRepository(Colaborador);
    this.leitosStatusService = new LeitosStatusService(ds);
  }

  /**
   * Sessão por Leito: cria avaliação cuja expiração é definida como o fim do dia
   * local (America/Sao_Paulo). A expiração diária é tratada pelo job de
   * sessionExpiry que roda à meia-noite.
   */
  async criarSessaoPorLeito(params: {
    leitoId: string;
    unidadeId: string;
    scp: string;
    itens: Record<string, number>;
    colaboradorId: string;
    prontuario?: string | null;
    dataAplicacao?: string; // yyyy-mm-dd format for historical data
    ifExists?: "reject" | "overwrite" | "replace";
  }) {
    // garantir atomicidade e evitar condições de corrida
    return await this.repo.manager.transaction(async (manager) => {
      const { leitoId, unidadeId, scp, itens, colaboradorId } = params;
      const prontuario = params.prontuario ?? null;
      const ifExists = params.ifExists ?? "overwrite";

      const unidade = await manager
        .getRepository(UnidadeInternacao)
        .findOneOrFail({
          where: { id: unidadeId },
          relations: ["hospital", "scpMetodo"],
        });

      const leito = await manager.getRepository(Leito).findOneOrFail({
        where: { id: leitoId },
        relations: ["unidade"],
      });
      if (leito.unidade.id !== unidade.id)
        throw new Error("Leito não pertence à unidade informada");
      if (leito.status === StatusLeito.INATIVO)
        throw new Error("Leito bloqueado");

      const expectedKey = unidade.scpMetodo?.key?.toUpperCase();
      if (!expectedKey)
        throw new Error("Unidade não tem método SCP configurado.");
      if (expectedKey !== scp.toUpperCase())
        throw new Error("SCP enviado difere do SCP configurado na unidade.");

      // valida itens contra schema dinâmico se existir
      const metodo = await manager.getRepository(ScpMetodo).findOne({
        where: { key: expectedKey },
      });
      if (metodo) {
        const validKeys = new Set(metodo.questions.map((q) => q.key));
        for (const k of Object.keys(itens))
          if (!validKeys.has(k))
            throw new Error(`Item inválido para método ${scp}: ${k}`);
      }

      const total = Object.values(itens).reduce(
        (a, b) => a + (Number(b) || 0),
        0
      );
      const classe = metodo
        ? (metodo.faixas.find((f) => total >= f.min && total <= f.max)
            ?.classe as ClassificacaoCuidado)
        : classificarTotal(expectedKey as any, total);
      const colaboradorRepo = manager.getRepository(Colaborador);

      let autor: Colaborador | null = await colaboradorRepo.findOne({
        where: { id: colaboradorId },
      });

      if (!autor) {
        throw new Error("Colaborador não encontrado");
      }

      // Usa a data LOCAL (America/Sao_Paulo) como dataAplicacao para evitar
      // discrepâncias em relação ao front e filtros por dia.
      const ZONE = "America/Sao_Paulo";
      const nowLocal = DateTime.now().setZone(ZONE);

      const dataHoje =
        params.dataAplicacao ||
        nowLocal.toISODate() ||
        new Date().toISOString().slice(0, 10);

      // For historical data, set expiration to the end of that specific day
      const targetDate = params.dataAplicacao
        ? DateTime.fromISO(params.dataAplicacao).setZone(ZONE)
        : nowLocal;
      const endLocalForToday = targetDate.endOf("day");
      const expiresAt = endLocalForToday.toUTC().toJSDate();
      // Verifica se já existe sessão ATIVA para o mesmo leito e dataAplicacao
      const existing = await manager.getRepository(AvaliacaoSCP).findOne({
        where: {
          leito: { id: leitoId },
          dataAplicacao: dataHoje,
          statusSessao: StatusSessaoAvaliacao.ATIVA,
        },
        relations: ["leito", "unidade", "autor"],
      });
      if (existing) {
        if (ifExists === "reject") {
          throw new Error(
            "Já existe uma sessão ativa neste leito para a data."
          );
        }

        if (ifExists === "overwrite") {
          // atualiza a sessão existente (mantém id)
          existing.itens = itens;
          existing.totalPontos = total;
          existing.classificacao = classe;
          existing.prontuario = prontuario;
          existing.scp = expectedKey;
          existing.expiresAt = expiresAt;
          existing.statusSessao = StatusSessaoAvaliacao.ATIVA;
          existing.autor = autor;

          // garante que o leito esteja marcado como ATIVO
          try {
            const leitoRepo = manager.getRepository(Leito);
            if (existing.leito) {
              existing.leito.status = StatusLeito.ATIVO;
              await leitoRepo.save(existing.leito);
            }
          } catch (e) {
            console.warn("Não foi possível atualizar status do leito:", e);
          }

          return await manager.getRepository(AvaliacaoSCP).save(existing);
        }

        if (ifExists === "replace") {
          // tenta remover a existente; em caso de falha, marca como liberada e segue
          try {
            await manager.getRepository(AvaliacaoSCP).remove(existing);
          } catch (e) {
            existing.statusSessao = StatusSessaoAvaliacao.LIBERADA;
            existing.expiresAt = new Date();
            await manager.getRepository(AvaliacaoSCP).save(existing);
          }
          // continua para criar uma nova sessão abaixo
        }
      }

      const avaliacao = manager.getRepository(AvaliacaoSCP).create({
        dataAplicacao: dataHoje as string,
        unidade,
        autor,
        scp: expectedKey,
        itens,
        totalPontos: total,
        classificacao: classe,
        leito,
        prontuario,
        expiresAt,
        statusSessao: StatusSessaoAvaliacao.ATIVA,
      });

      // garante que o leito esteja marcado como ATIVO (usando o mesmo manager)
      try {
        const leitoRepo = manager.getRepository(Leito);
        leito.status = StatusLeito.ATIVO;
        await leitoRepo.save(leito);
      } catch (e) {
        console.warn("Não foi possível atualizar status do leito:", e);
      }

      const saved = await manager.getRepository(AvaliacaoSCP).save(avaliacao);

      // Atualiza leitos_status da unidade após criar/atualizar avaliação
      try {
        await this.leitosStatusService.atualizarStatusUnidade(unidadeId);
      } catch (e) {
        console.warn("Não foi possível atualizar leitos_status:", e);
      }

      return saved;
    });
  }

  async liberarSessao(avaliacaoId: string) {
    const av = await this.repo.findOneOrFail({
      where: { id: avaliacaoId },
      relations: ["leito"],
    });
    if (av.statusSessao !== StatusSessaoAvaliacao.ATIVA) return av;
    av.statusSessao = StatusSessaoAvaliacao.LIBERADA;
    av.expiresAt = av.expiresAt ?? new Date();
    // ao liberar, marcar leito como PENDENTE
    try {
      const leitoRepo = this.repo.manager.getRepository(Leito);
      if (av.leito) {
        av.leito.status = StatusLeito.PENDENTE;
        await leitoRepo.save(av.leito);
      }
    } catch (e) {
      console.warn("Não foi possível atualizar status do leito ao liberar:", e);
    }

    const saved = await this.repo.save(av);

    // Atualiza leitos_status da unidade após liberar sessão
    if (av.unidade?.id) {
      try {
        await this.leitosStatusService.atualizarStatusUnidade(av.unidade.id);
      } catch (e) {
        console.warn("Não foi possível atualizar leitos_status:", e);
      }
    }

    return saved;
  }

  /**
   * Atualiza uma sessão/avaliação existente.
   * params pode conter: itens, colaboradorId (novo autor), prontuario e scp (opcional).
   * Recalcula total/classificação e atualiza expiresAt para o fim do dia.
   */
  async atualizarSessao(
    avaliacaoId: string,
    params: {
      itens?: Record<string, number>;
      colaboradorId?: string;
      prontuario?: string | null;
      scp?: string;
    }
  ) {
    const { itens, colaboradorId, prontuario = null, scp } = params;
    console.log(avaliacaoId);

    const av = await this.repo.findOne({
      where: { id: avaliacaoId },
      relations: ["leito", "unidade", "autor"],
    });
    if (!av) throw new Error("Avaliação não encontrada");
    console.log(scp);
    // decide o SCP efetivo (param ou existente)
    const effectiveScp = scp ? scp.toUpperCase() : av.scp;
    console.log(effectiveScp);
    // valida SCP contra o método configurado na unidade
    const unidade = av.unidade;
    console.log(unidade);
    const expectedKey = effectiveScp;

    // valida itens contra schema dinâmico se existir
    const metodo = await this.scpMetodoRepo.findOne({
      where: { key: expectedKey },
    });
    if (metodo && itens) {
      const validKeys = new Set(metodo.questions.map((q) => q.key));
      for (const k of Object.keys(itens))
        if (!validKeys.has(k))
          throw new Error(`Item inválido para método ${effectiveScp}: ${k}`);
    }

    // atualiza autor se fornecido
    if (colaboradorId) {
      const autor = await this.colaboradorRepo.findOneOrFail({
        where: { id: colaboradorId },
      });
      av.autor = autor;
    }

    // atualiza itens/prontuario/scp
    if (itens) av.itens = itens;
    av.prontuario = prontuario ?? av.prontuario;
    av.scp = effectiveScp;

    // recalc total e classificacao
    const total = Object.values(av.itens || {}).reduce(
      (a, b) => a + (Number(b) || 0),
      0
    );
    const classe = metodo
      ? (metodo.faixas.find((f) => total >= f.min && total <= f.max)
          ?.classe as ClassificacaoCuidado)
      : classificarTotal(effectiveScp as any, total);

    av.totalPontos = total;
    av.classificacao = classe;

    // redefine expiresAt para o fim do dia local
    const ZONE = "America/Sao_Paulo";
    const nowLocal = DateTime.now().setZone(ZONE);
    const dataHoje = nowLocal.toISODate();
    const endLocalForToday = nowLocal.endOf("day");
    av.expiresAt = endLocalForToday.toUTC().toJSDate();

    av.statusSessao = StatusSessaoAvaliacao.ATIVA;

    // garante que o leito esteja marcado como ATIVO
    try {
      const leitoRepo = this.repo.manager.getRepository(Leito);
      if (av.leito) {
        av.leito.status = StatusLeito.ATIVO;
        await leitoRepo.save(av.leito);
      }
    } catch (e) {
      console.warn(
        "Não foi possível atualizar status do leito ao atualizar sessão:",
        e
      );
    }

    const saved = await this.repo.save(av);

    // Atualiza leitos_status da unidade após atualizar sessão
    if (av.unidade?.id) {
      try {
        await this.leitosStatusService.atualizarStatusUnidade(av.unidade.id);
      } catch (e) {
        console.warn("Não foi possível atualizar leitos_status:", e);
      }
    }

    return saved;
  }

  async listarSessoesAtivasPorUnidade(unidadeId: string) {
    return this.repo.find({
      where: {
        unidade: { id: unidadeId },
        statusSessao: StatusSessaoAvaliacao.ATIVA,
      },
      relations: ["leito", "autor"],
    });
  }

  async listarLeitosDisponiveisPorUnidade(unidadeId: string) {
    const leiRepo = this.repo.manager.getRepository(Leito);
    const leitos = await leiRepo.find({
      where: { unidade: { id: unidadeId } },
    });
    const ativas = await this.listarSessoesAtivasPorUnidade(unidadeId);
    const ocupados = new Set(
      ativas.map((a) => a.leito?.id).filter(Boolean) as string[]
    );
    return leitos.filter(
      (l) => !ocupados.has(l.id) && l.status !== StatusLeito.INATIVO
    );
  }

  listarTodas() {
    return this.repo.find({
      relations: ["unidade", "autor", "leito"],
      order: { dataAplicacao: "DESC", created_at: "DESC" },
    });
  }

  listarPorDia({ data, unidadeId }: FiltroAvaliacaoDTO) {
    // data é yyyy-mm-dd e representa dia em UTC
    return this.repo.find({
      where: { dataAplicacao: data, unidade: { id: unidadeId } },
      relations: ["unidade", "autor", "leito"],
      order: { created_at: "ASC" },
    });
  }

  // No AvaliacaoRepository
  listarPorUnidade = async (unidadeId: string) => {
    return await this.repo.find({
      where: { unidade: { id: unidadeId } },
      relations: ["unidade", "autor", "leito"],
      order: { dataAplicacao: "DESC" },
    });
  };

  async resumoDiario({
    data,
    unidadeId,
  }: FiltroAvaliacaoDTO): Promise<ResumoDiarioDTO> {
    const unidade = await this.unidadeRepo.findOneOrFail({
      where: { id: unidadeId },
      relations: ["leitos"],
    });
    const avals = await this.listarPorDia({ data, unidadeId });

    const totalOcupados = avals.length;
    const numeroLeitos = unidade.leitos.length;
    const taxaOcupacao = numeroLeitos > 0 ? totalOcupados / numeroLeitos : 0;

    const distribuicao: Partial<Record<ClassificacaoCuidado, number>> = {};
    for (const a of avals) {
      distribuicao[a.classificacao] = (distribuicao[a.classificacao] || 0) + 1;
    }

    return {
      data,
      unidadeId,
      totalOcupados,
      numeroLeitos,
      taxaOcupacao,
      distribuicao,
    };
  }

  async consolidadoMensal(unidadeId: string, ano: number, mes1a12: number) {
    const unidade = await this.unidadeRepo.findOneOrFail({
      where: { id: unidadeId },
      relations: ["leitos"],
    });

    const start = new Date(Date.UTC(ano, mes1a12 - 1, 1));
    const end = new Date(Date.UTC(ano, mes1a12, 1)); // exclusiv
    const dataIni = start.toISOString().slice(0, 10);
    const dataFim = end.toISOString().slice(0, 10);

    const avals = await this.repo.find({
      where: {
        dataAplicacao: Between(dataIni, dataFim),
        unidade: { id: unidadeId },
      },
    });

    // mapa por dia
    const porDia = new Map<string, AvaliacaoSCP[]>();
    for (const a of avals) {
      const arr = porDia.get(a.dataAplicacao) || [];
      arr.push(a);
      porDia.set(a.dataAplicacao, arr);
    }

    let somaOcupados = 0;
    const distGeral: Record<string, number> = {};
    let diasComMedicao = 0;

    for (const [, arr] of porDia) {
      diasComMedicao++;
      somaOcupados += arr.length;
      for (const a of arr) {
        distGeral[a.classificacao] = (distGeral[a.classificacao] || 0) + 1;
      }
    }

    const mediaOcupadosDia = diasComMedicao ? somaOcupados / diasComMedicao : 0;
    const taxaOcupacaoMedia = unidade.leitos.length
      ? mediaOcupadosDia / unidade.leitos.length
      : 0;

    return {
      ano,
      mes: mes1a12,
      unidadeId,
      diasComMedicao,
      mediaOcupadosDia,
      taxaOcupacaoMedia,
      distribuicaoMensal: distGeral,
    };
  }

  async buscarPorAutor(colaboradorId: string) {
    return await this.repo.find({
      where: {
        autor: { id: colaboradorId },
      },
      relations: ["autor"], // se quiser trazer os dados do colaborador também
      order: {
        created_at: "DESC", // opcional: ordena pela data
      },
    });
  }

  /** Lista avaliações no período [dataIni, dataFimExclusiva) com autor e outras relações úteis */
  async listarNoPeriodoComAutor(
    unidadeId: string,
    dataIni: string,
    dataFimExclusiva: string
  ) {
    return this.repo.find({
      where: {
        dataAplicacao: Between(dataIni, dataFimExclusiva),
        unidade: { id: unidadeId },
      },
      relations: ["autor", "unidade", "leito"],
      order: { dataAplicacao: "ASC", created_at: "ASC" },
    });
  }
}
