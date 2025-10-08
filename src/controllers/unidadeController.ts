import { Request, Response } from "express";
import { UnidadeRepository } from "../repositories/unidadeRepository";
import { AvaliacaoRepository } from "../repositories/avaliacaoRepository";
import { LeitoRepository } from "../repositories/leitoRepository";
import { HistoricoOcupacaoRepository } from "../repositories/historicoOcupacaoRepository";
import { DataSource } from "typeorm";
import { DateTime } from "luxon";
import { StatusLeito } from "../entities/Leito";
import { StatusSessaoAvaliacao } from "../entities/AvaliacaoSCP";

export class UnidadeController {
  private ds: DataSource;

  constructor(private repo: UnidadeRepository, ds?: DataSource) {
    this.ds = ds!;
  }

  criar = async (req: Request, res: Response) => {
    try {
      const novo = await this.repo.criar(req.body);
      res.status(201).json(novo);
    } catch (err: any) {
      console.error("Erro ao criar unidade:", err);
      const message = err.message || "Erro ao criar unidade";
      return res.status(400).json({ mensagem: message });
    }
  };

  listar = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.query as { hospitalId?: string };
      const itens = await this.repo.listar(hospitalId);
      // Sanitize possible BigInt fields for JSON serialization (e.g., hospital.telefone)
      const sane = itens.map((u: any) => {
        if (u?.hospital && typeof (u.hospital as any).telefone === "bigint") {
          u = {
            ...u,
            hospital: {
              ...u.hospital,
              telefone: (u.hospital as any).telefone.toString(),
            },
          };
        }
        return u;
      });
      return res.json(sane);
    } catch (err) {
      console.error("Erro ao listar unidades:", err);
      return res.status(500).json({ mensagem: "Erro ao listar unidades" });
    }
  };

  obter = async (req: Request, res: Response) => {
    const { id } = req.params as { id?: string };
    if (!id) return res.status(400).json({ mensagem: "id obrigatório" });
    // validação simples de UUID v4 (formato) – evita query com 'undefined'
    const uuidRegex =
      /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
    if (!uuidRegex.test(id))
      return res.status(400).json({ mensagem: "id inválido" });
    const item = await this.repo.obter(id);
    if (!item)
      return res.status(404).json({ mensagem: "Unidade não encontrada" });
    return res.json(item);
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const up = await this.repo.atualizar(req.params.id, req.body);
      res.json(up);
    } catch (err: any) {
      console.error("Erro ao atualizar unidade:", err);
      const message = err.message || "Erro ao atualizar unidade";
      return res.status(400).json({ mensagem: message });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const ok = await this.repo.deletar(req.params.id);
      return ok
        ? res.status(204).send()
        : res.status(404).json({ mensagem: "Unidade não encontrada" });
    } catch (err: any) {
      console.error("Erro ao deletar unidade:", err);
      const message = err.message || "Erro ao deletar unidade";
      return res.status(400).json({ mensagem: message });
    }
  };

  estatisticasConsolidadas = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(id)) {
        return res.status(400).json({ mensagem: "ID inválido" });
      }

      // Verificar se unidade existe
      const unidade = await this.repo.obter(id);
      if (!unidade) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      const leitoRepo = new LeitoRepository(this.ds);
      const avaliacaoRepo = new AvaliacaoRepository(this.ds);
      const historicoRepo = new HistoricoOcupacaoRepository(this.ds);

      // 1. Contagem de leitos por status (atual)
      const leitos = await leitoRepo.listar(id);
      const leitosStatus = {
        total: leitos.length,
        ativo: leitos.filter((l) => (l as any).status === StatusLeito.ATIVO)
          .length,
        vago: leitos.filter((l) => (l as any).status === StatusLeito.VAGO)
          .length,
        pendente: leitos.filter(
          (l) => (l as any).status === StatusLeito.PENDENTE
        ).length,
        inativo: leitos.filter((l) => (l as any).status === StatusLeito.INATIVO)
          .length,
      };

      // 2. Ocupação atual (sessões ativas)
      const sessoesAtivas = await avaliacaoRepo.listarSessoesAtivasPorUnidade(
        id
      );
      const ocupados = sessoesAtivas.length;
      const leitosDisponiveis = leitosStatus.total; // Usar todos os leitos, não excluir inativos
      // Taxa de ocupação como decimal 0..1 (ex: 0.75) — arredondada para 2 casas
      const taxaOcupacao =
        leitosDisponiveis > 0 ? ocupados / leitosDisponiveis : 0;

      // 3. Distribuição SCP do mês atual
      const now = DateTime.now().setZone("America/Sao_Paulo");
      const ano = now.year;
      const mes = now.month;

      // Consolidado mensal de leitos
      // Calcular para cada dia do mês:
      // leitosOperacionaisDia, totalLeitosDia, leitosOcupadosDia
      const diasNoMes = new Date(ano, mes, 0).getDate();
      const hoje = now.day; // Dia atual do mês
      const limiteDia = diasNoMes > hoje ? hoje : diasNoMes; // Para mês atual, só até hoje
      let leitosOperacionaisMes = 0;
      let totalLeitosMes = 0;
      let leitosOcupadosMes = 0;

      // Debug array para rastrear contagem por dia
      const debugDias = [];

      for (let dia = 1; dia <= limiteDia; dia++) {
        const dataISO = `${ano}-${String(mes).padStart(2, "0")}-${String(
          dia
        ).padStart(2, "0")}`;

        // Buscar avaliações reais do dia
        const avalsDia = await avaliacaoRepo.listarPorDia({
          data: dataISO,
          unidadeId: id,
        });

        // CORREÇÃO: Total de leitos sempre é o número atual cadastrado
        const totalLeitosDia = leitos.length;

        // CORREÇÃO: Buscar apenas leitos que estavam INATIVOS no histórico
        const historicosDia = await historicoRepo.listarPorDia(dataISO, id);
        const leitosInativosDia = new Set();

        for (const hist of historicosDia) {
          if (hist.leitoStatus === "INATIVO") {
            leitosInativosDia.add(hist.leito?.id || hist.leitoNumero);
          }
        }

        // Usar todos os leitos para cálculo (não subtrair inativos)
        const leitosOperacionaisDia = totalLeitosDia; // Mudança: usar todos os leitos
        const leitosOcupadosDia = avalsDia.length;

        totalLeitosMes += totalLeitosDia;
        leitosOperacionaisMes += leitosOperacionaisDia;
        leitosOcupadosMes += leitosOcupadosDia;

        // Debug para rastreamento
        debugDias.push({
          data: dataISO,
          totalLeitos: totalLeitosDia,
          leitosInativos: leitosInativosDia.size,
          leitosOperacionais: leitosOperacionaisDia,
          leitosOcupados: leitosOcupadosDia,
          historicoRegistros: historicosDia.length,
        });
      }

      console.log(
        `=== DEBUG CONSOLIDADO MENSAL - Unidade ${id.slice(0, 8)} ===`
      );
      console.log(`Período: ${limiteDia} dias do mês ${mes}/${ano}`);
      console.log(`Breakdown por dia:`, debugDias);
      console.log(
        `Totais acumulados: Total=${totalLeitosMes}, Operacionais=${leitosOperacionaisMes}, Ocupados=${leitosOcupadosMes}`
      );

      // Taxa média como decimal 0..1, com até 2 casas decimais
      const taxaOcupacaoMedia =
        totalLeitosMes > 0
          ? parseFloat((leitosOcupadosMes / totalLeitosMes).toFixed(2))
          : 0;

      // Distribuição SCP do mês (mantém chamada ao consolidadoMensal se existir)
      let distribuicaoSCP = {
        MINIMOS: 0,
        INTERMEDIARIOS: 0,
        ALTA_DEPENDENCIA: 0,
        SEMI_INTENSIVOS: 0,
        INTENSIVOS: 0,
      };
      if (avaliacaoRepo.consolidadoMensal) {
        try {
          const consolidado = await avaliacaoRepo.consolidadoMensal(
            id,
            ano,
            mes
          );
          distribuicaoSCP = {
            MINIMOS: consolidado.distribuicaoMensal.MINIMOS || 0,
            INTERMEDIARIOS: consolidado.distribuicaoMensal.INTERMEDIARIOS || 0,
            ALTA_DEPENDENCIA:
              consolidado.distribuicaoMensal.ALTA_DEPENDENCIA || 0,
            SEMI_INTENSIVOS:
              consolidado.distribuicaoMensal.SEMI_INTENSIVOS || 0,
            INTENSIVOS: consolidado.distribuicaoMensal.INTENSIVOS || 0,
          };
        } catch {}
      }

      const response = {
        leitosStatus,
        ocupacao: {
          ocupados,
          // Retornar decimal com até 2 casas (ex: 0.75)
          taxaOcupacao: parseFloat(taxaOcupacao.toFixed(2)),
          taxaOcupacaoMedia,
          leitosOperacionaisMes,
          totalLeitosMes,
          leitosOcupadosMes,
        },
        distribuicaoSCP,
        periodo: {
          mes,
          ano,
        },
      };

      return res.json(response);
    } catch (error) {
      console.error("Erro ao obter estatísticas consolidadas:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: (error as Error).message,
      });
    }
  };

  resumoMensal = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const { ano, mes, incluirDetalhes } = req.query as {
        ano?: string;
        mes?: string;
        incluirDetalhes?: string;
      };

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(id)) {
        return res.status(400).json({ mensagem: "ID inválido" });
      }

      // Verificar se unidade existe
      const unidade = await this.repo.obter(id);
      if (!unidade) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      // Usar mês/ano atual se não informado (timezone São Paulo)
      const now = DateTime.now().setZone("America/Sao_Paulo");
      const anoNum = ano ? parseInt(ano, 10) : now.year;
      const mesNum = mes ? parseInt(mes, 10) : now.month;

      if (isNaN(anoNum) || isNaN(mesNum) || mesNum < 1 || mesNum > 12) {
        return res.status(400).json({ mensagem: "Ano ou mês inválido" });
      }

      const leitoRepo = new LeitoRepository(this.ds);
      const avaliacaoRepo = new AvaliacaoRepository(this.ds);
      const historicoRepo = new HistoricoOcupacaoRepository(this.ds);

      // Obter total de leitos cadastrados
      const leitos = await leitoRepo.listar(id);
      const totalLeitos = leitos.length;

      // Calcular dias do mês
      const diasNoMes = new Date(anoNum, mesNum, 0).getDate();
      const hoje = now.toISODate(); // yyyy-mm-dd
      const mesAtual = now.year === anoNum && now.month === mesNum;

      const dias = [];

      for (let dia = 1; dia <= diasNoMes; dia++) {
        const dataISO = `${anoNum}-${String(mesNum).padStart(2, "0")}-${String(
          dia
        ).padStart(2, "0")}`;

        // Para o mês atual, só incluir dias até hoje
        if (mesAtual && dataISO > hoje!) {
          continue; // Não incluir dias futuros do mês atual
        }

        // Buscar avaliações reais do dia
        const avalsDia = await avaliacaoRepo.listarPorDia({
          data: dataISO,
          unidadeId: id,
        });

        // Se não há avaliações para este dia, pular
        if (avalsDia.length === 0) {
          continue;
        }

        const avaliacoesEfetivas = avalsDia;

        // Distribuição por classificação
        const distribuicao = {
          minimos: 0,
          intermediarios: 0,
          altaDependencia: 0,
          semiIntensivos: 0,
          intensivos: 0,
        };

        for (const aval of avaliacoesEfetivas) {
          const cls = (aval as any).classificacao?.toLowerCase() || "";
          switch (cls) {
            case "minimos":
              distribuicao.minimos++;
              break;
            case "intermediarios":
              distribuicao.intermediarios++;
              break;
            case "alta_dependencia":
              distribuicao.altaDependencia++;
              break;
            case "semi_intensivos":
              distribuicao.semiIntensivos++;
              break;
            case "intensivos":
              distribuicao.intensivos++;
              break;
          }
        }

        // CORREÇÃO: Buscar leitos inativos para esta data específica usando histórico
        const leitosInativosDia =
          await historicoRepo.contarLeitosInativosPorDia(dataISO, id);

        // Estatísticas de leitos para o dia
        const leitosOcupados = avaliacoesEfetivas.length;
        const leitosAtivos = totalLeitos; // Usar todos os leitos, não subtrair inativos
        const leitosVagos = Math.max(0, leitosAtivos - leitosOcupados);
        const taxaOcupacao =
          leitosAtivos > 0
            ? Math.round((leitosOcupados / leitosAtivos) * 100)
            : 0;

        // Formatação da data
        const dt = DateTime.fromISO(dataISO, { zone: "America/Sao_Paulo" });
        const dataFormatada =
          dt.toFormat("dd/MM") +
          " - " +
          ["dom", "seg", "ter", "qua", "qui", "sex", "sab"][dt.weekday % 7];

        const diaData = {
          data: dataISO,
          dataFormatada,
          isHoje: dataISO === hoje,
          quantidadeAvaliacoes: avaliacoesEfetivas.length,
          distribuicao,
          estatisticas: {
            totalLeitos,
            leitosOcupados,
            leitosVagos,
            leitosPendentes: Math.max(0, leitosAtivos - leitosOcupados), // Assumindo PENDENTE = não ocupado
            leitosInativos: leitosInativosDia, // Usar quantidade histórica do dia específico
            taxaOcupacao,
          },
        };

        // Incluir detalhes se solicitado
        if (incluirDetalhes === "true") {
          // Colaboradores que fizeram avaliações
          const colaboradores = new Map();
          for (const aval of avaliacoesEfetivas) {
            const autor = (aval as any).autor;
            if (autor) {
              const autorId = autor.id;
              if (!colaboradores.has(autorId)) {
                colaboradores.set(autorId, {
                  id: autorId,
                  nome: autor.nome,
                  total: 0,
                  distribuicao: { ...distribuicao },
                });
              }
              colaboradores.get(autorId).total++;

              const cls = (aval as any).classificacao?.toLowerCase() || "";
              switch (cls) {
                case "minimos":
                  colaboradores.get(autorId).distribuicao.minimos++;
                  break;
                case "intermediarios":
                  colaboradores.get(autorId).distribuicao.intermediarios++;
                  break;
                case "alta_dependencia":
                  colaboradores.get(autorId).distribuicao.altaDependencia++;
                  break;
                case "semi_intensivos":
                  colaboradores.get(autorId).distribuicao.semiIntensivos++;
                  break;
                case "intensivos":
                  colaboradores.get(autorId).distribuicao.intensivos++;
                  break;
              }
            }
          }

          (diaData as any).detalhes = {
            colaboradores: Array.from(colaboradores.values()),
            avaliacoes: avaliacoesEfetivas,
          };
        }

        dias.push(diaData);
      }

      // Ordenar dias mais recentes primeiro
      dias.sort((a, b) => b.data.localeCompare(a.data));

      const response = {
        unidadeId: id,
        nomeUnidade: unidade.nome,
        ano: anoNum,
        mes: mesNum,
        dias,
      };

      return res.json(response);
    } catch (error) {
      console.error("Erro ao obter resumo mensal:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: (error as Error).message,
      });
    }
  };

  historicoMensal = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const { dataInicial, dataFinal } = req.query as {
        dataInicial?: string;
        dataFinal?: string;
      };

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(id)) {
        return res.status(400).json({ mensagem: "ID inválido" });
      }

      // Verificar se unidade existe
      const unidade = await this.repo.obter(id);
      if (!unidade) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      // Validar parâmetros de data
      if (!dataInicial || !dataFinal) {
        return res.status(400).json({
          mensagem:
            "dataInicial e dataFinal são obrigatórios no formato YYYY-MM",
        });
      }

      const regexData = /^\d{4}-\d{2}$/;
      if (!regexData.test(dataInicial) || !regexData.test(dataFinal)) {
        return res.status(400).json({
          mensagem: "Formato de data inválido. Use YYYY-MM",
        });
      }

      const [anoInicial, mesInicial] = dataInicial.split("-").map(Number);
      const [anoFinal, mesFinal] = dataFinal.split("-").map(Number);

      if (mesInicial < 1 || mesInicial > 12 || mesFinal < 1 || mesFinal > 12) {
        return res
          .status(400)
          .json({ mensagem: "Mês deve estar entre 01 e 12" });
      }

      const leitoRepo = new LeitoRepository(this.ds);
      const avaliacaoRepo = new AvaliacaoRepository(this.ds);
      const historicoRepo = new HistoricoOcupacaoRepository(this.ds);

      // Obter leitos da unidade (para usar como fallback)
      const leitos = await leitoRepo.listar(id);

      const historico = [];

      // Iterar por cada mês no intervalo
      let anoAtual = anoInicial;
      let mesAtual = mesInicial;

      while (
        anoAtual < anoFinal ||
        (anoAtual === anoFinal && mesAtual <= mesFinal)
      ) {
        const mesAno = `${String(mesAtual).padStart(2, "0")}/${anoAtual}`;

        // Calcular dias do mês
        const diasNoMes = new Date(anoAtual, mesAtual, 0).getDate();

        // Para o mês atual, considerar apenas dias até hoje
        const now = DateTime.now().setZone("America/Sao_Paulo");
        const ehMesAtual = anoAtual === now.year && mesAtual === now.month;
        const limiteDia = ehMesAtual ? Math.min(diasNoMes, now.day) : diasNoMes;

        // Contadores mensais usando EXATAMENTE a mesma lógica da Taxa de Ocupação Mensal
        let cuidadosMinimos = 0;
        let cuidadosIntermediarios = 0;
        let cuidadosAltaDependencia = 0;
        let cuidadosSemiIntensivos = 0;
        let cuidadosIntensivos = 0;

        // Campos que devem ter a mesma lógica da Taxa de Ocupação Mensal
        let leitosOperacionaisMes = 0;
        let totalLeitosMes = 0;
        let leitosOcupadosMes = 0;

        // Processar cada dia do mês usando a MESMA lógica do estatisticasConsolidadas
        for (let dia = 1; dia <= limiteDia; dia++) {
          const dataISO = `${anoAtual}-${String(mesAtual).padStart(
            2,
            "0"
          )}-${String(dia).padStart(2, "0")}`;

          // Buscar avaliações do dia
          const avalsDia = await avaliacaoRepo.listarPorDia({
            data: dataISO,
            unidadeId: id,
          });

          // Usar MESMA lógica corrigida do estatisticasConsolidadas
          const totalLeitosDia = leitos.length; // Sempre o número atual de leitos cadastrados
          const leitosInativosDia =
            await historicoRepo.contarLeitosInativosPorDia(dataISO, id);
          const leitosOperacionaisDia = totalLeitosDia - leitosInativosDia; // Total menos os inativos
          const leitosOcupadosDia = avalsDia.length;

          totalLeitosMes += totalLeitosDia;
          leitosOperacionaisMes += leitosOperacionaisDia;
          leitosOcupadosMes += leitosOcupadosDia;

          // Contar classificações
          for (const aval of avalsDia) {
            const classificacao = (aval as any).classificacao;
            switch (classificacao) {
              case "MINIMOS":
                cuidadosMinimos++;
                break;
              case "INTERMEDIARIOS":
                cuidadosIntermediarios++;
                break;
              case "ALTA_DEPENDENCIA":
                cuidadosAltaDependencia++;
                break;
              case "SEMI_INTENSIVOS":
                cuidadosSemiIntensivos++;
                break;
              case "INTENSIVOS":
                cuidadosIntensivos++;
                break;
            }
          }
        }

        // Usar EXATAMENTE a mesma fórmula da Taxa de Ocupação Mensal
        const taxaOcupacaoMedia =
          totalLeitosMes > 0
            ? Math.round((leitosOcupadosMes / totalLeitosMes) * 100)
            : 0;

        historico.push({
          mesAno,
          cuidadosMinimos,
          cuidadosIntermediarios,
          cuidadosAltaDependencia,
          cuidadosSemiIntensivos,
          cuidadosIntensivos,
          somaLeitos: totalLeitosMes, // = totalLeitosMes da Taxa de Ocupação Mensal
          leitosOperacionais: leitosOperacionaisMes, // = totalLeitosMes (todos os leitos)
          percentualOcupacao: taxaOcupacaoMedia, // = taxaOcupacaoMedia da Taxa de Ocupação Mensal
        });

        // Próximo mês
        mesAtual++;
        if (mesAtual > 12) {
          mesAtual = 1;
          anoAtual++;
        }
      }

      return res.json(historico);
    } catch (error) {
      console.error("Erro ao obter histórico mensal:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: (error as Error).message,
      });
    }
  };
}
