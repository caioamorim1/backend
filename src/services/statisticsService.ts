import { DataSource } from "typeorm";
import { HistoricoOcupacaoRepository } from "../repositories/historicoOcupacaoRepository";
import { AvaliacaoRepository } from "../repositories/avaliacaoRepository";
import { LeitoRepository } from "../repositories/leitoRepository";
import { UnidadeRepository } from "../repositories/unidadeRepository";
import { ClassificacaoCuidado } from "../entities/AvaliacaoSCP";
import { StatusLeito } from "../entities/Leito";
import { pdfResumoDiario, pdfMensal } from "../utils/exporters/pdf";

export class StatisticsService {
  constructor(private ds: DataSource) {}

  // Estatísticas de uma unidade dentro de um período (dataIni/dataFim no formato yyyy-mm-dd)
  async unidadeStats(unidadeId: string, dataIni?: string, dataFim?: string) {
    const unidadeRepo = new UnidadeRepository(this.ds);
    const leitoRepo = new LeitoRepository(this.ds);
    const avalRepo = new AvaliacaoRepository(this.ds);
    const histRepo = new HistoricoOcupacaoRepository(this.ds);

    const unidade = await unidadeRepo.obter(unidadeId);
    if (!unidade) throw new Error("Unidade não encontrada");

    // leitos e status (usar todos os leitos cadastrados)
    const leitos = await leitoRepo.listar(unidadeId);
    const statusCounts: Record<string, number> = {};
    for (const l of leitos)
      statusCounts[(l as any).status] =
        (statusCounts[(l as any).status] || 0) + 1;
    // total de leitos = todos os leitos cadastrados
    const totalLeitos = leitos.length;

    // periodo para consultas (se omitido, usa dataAplicacao = hoje)
    let dataStart = dataIni;
    let dataEnd = dataFim;
    if (!dataStart && !dataEnd) {
      const { DateTime } = require("luxon");
      const todaySp = DateTime.now().setZone("America/Sao_Paulo").toISODate();
      dataStart = dataEnd = todaySp || new Date().toISOString().slice(0, 10);
    }
    if (dataStart && !dataEnd) dataEnd = dataStart;

    // avaliações no período
    const avals = await avalRepo.listarNoPeriodoComAutor(
      unidadeId,
      dataStart!,
      nextDateExclusive(dataEnd!)
    );

    // distribuição por classificação
    const distribuicao: Record<string, number> = {};
    const byColaborador: Record<
      string,
      { nome: string; total: number; distribuicao: Record<string, number> }
    > = {};
    for (const a of avals) {
      const cls = a.classificacao || "-";
      distribuicao[cls] = (distribuicao[cls] || 0) + 1;
      if (a.autor) {
        const id = (a.autor as any).id;
        if (!byColaborador[id])
          byColaborador[id] = {
            nome: (a.autor as any).nome || "-",
            total: 0,
            distribuicao: {},
          };
        byColaborador[id].total += 1;
        byColaborador[id].distribuicao[cls] =
          (byColaborador[id].distribuicao[cls] || 0) + 1;
      }
    }

    // historico para ocupação média no período
    const historicos = await histRepo.listarPorPeriodo(
      dataStart!,
      dataEnd!,
      unidadeId
    );
    // agrupa por dia (yyyy-mm-dd) e conta leitos únicos ocupados por dia
    const ocupacaoPorDia = new Map<string, Set<string>>();
    for (const h of historicos) {
      const dia = (h.inicio as Date).toISOString().slice(0, 10);
      const set = ocupacaoPorDia.get(dia) || new Set<string>();
      if (h.leito && (h.leito as any).id) set.add((h.leito as any).id);
      ocupacaoPorDia.set(dia, set);
    }
    const dias = Array.from(ocupacaoPorDia.keys());
    const somaOcupados = dias.reduce(
      (s, d) => s + (ocupacaoPorDia.get(d)?.size || 0),
      0
    );
    const mediaOcupadosDia = dias.length ? somaOcupados / dias.length : 0;
    const taxaOcupacaoMedia = totalLeitos ? mediaOcupadosDia / totalLeitos : 0;

    return {
      unidade: {
        id: unidade.id,
        nome: unidade.nome,
        scpMetodoKey: (unidade as any).scpMetodoKey ?? null,
      },
      periodo: { dataIni: dataStart, dataFim: dataEnd },
      totalLeitos,
      statusCounts,
      distribuicao,
      colaboradores: Object.values(byColaborador),
      ocupacao: {
        mediaOcupadosDia,
        taxaOcupacaoMedia,
        dias: dias.map((d) => ({
          data: d,
          ocupados: ocupacaoPorDia.get(d)?.size || 0,
        })),
      },
      avaliacoes: avals,
    };
  }

  // Hospital-level stats: agrega por unidade
  async hospitalStats(hospitalId: string, dataIni?: string, dataFim?: string) {
    const unidadeRepo = new UnidadeRepository(this.ds);
    const unidades = await unidadeRepo.listarPorHospital(hospitalId);
    const results = [] as any[];
    for (const u of unidades) {
      const stats = await this.unidadeStats(u.id, dataIni, dataFim);
      results.push(stats);
    }
    // agregação simples
    const totalLeitos = results.reduce((s, r) => s + (r.totalLeitos || 0), 0);
    const somaMedia = results.reduce(
      (s, r) => s + (r.ocupacao.mediaOcupadosDia || 0),
      0
    );
    const taxaMedia = totalLeitos ? somaMedia / totalLeitos : 0;
    return {
      hospitalId,
      periodo: { dataIni, dataFim },
      unidades: results,
      totalLeitos,
      taxaOcupacaoMedia: taxaMedia,
    };
  }

  // Exporta PDF: para unidade, cria resumo diário ou mensal conforme periodo
  async exportUnidadePdf(
    unidadeId: string,
    dataIni?: string,
    dataFim?: string
  ) {
    const stats = await this.unidadeStats(unidadeId, dataIni, dataFim);

    // Decide se é diário (mesmo dia) ou mensal (intervalo)
    const sameDay = !!dataIni && !!dataFim && dataIni === dataFim;

    if (sameDay) {
      // --------- DIÁRIO ----------
      // Se não houver histórico para o dia, cai no número de avaliações do próprio dia
      const ocupadosDia =
        (stats?.ocupacao?.dias?.[0]?.ocupados ??
          (Array.isArray(stats?.avaliacoes) ? stats.avaliacoes.length : 0)) ||
        0;

      // totalLeitos deve representar todos os leitos (não apenas ativos)
      const totalLeitos = Number(stats?.totalLeitos ?? 0);

      // Taxa diária = ocupados do dia / total de leitos
      const taxaOcupacaoDia = totalLeitos > 0 ? ocupadosDia / totalLeitos : 0;

      return await pdfResumoDiario({
        data: dataIni!, // já garantimos sameDay com dataIni definido
        unidade: stats?.unidade?.nome ?? "",
        numeroLeitos: totalLeitos,
        ocupacao: { usada: ocupadosDia },
        taxaOcupacao: taxaOcupacaoDia, // << aqui é a correção
        distribuicao: stats?.distribuicao ?? {},
        avaliacoes: (stats?.avaliacoes ?? []).map((a: any) => ({
          leito: a?.leito ?? {},
          created_at: a?.created_at,
          autor: a?.autor ?? {},
          classificacao: a?.classificacao,
        })),
      });
    }

    // se mensal (dataIni,dataFim diferentes) -> usar pdfMensal simplificado
    const ano = Number(dataIni?.slice(0, 4) || new Date().getFullYear());
    const mes = Number(dataIni?.slice(5, 7) || new Date().getMonth() + 1);
    const buf = await pdfMensal({
      ano,
      mes,
      unidade: stats.unidade.nome,
      numeroLeitos: stats.totalLeitos,
      ocupacaoMensal: {
        mediaOcupadosDia: stats.ocupacao.mediaOcupadosDia,
        taxaOcupacaoMedia: stats.ocupacao.taxaOcupacaoMedia,
        avaliacao: stats.ocupacao.dias,
      },
      distribuicaoMensal: stats.distribuicao,
      colaboradores: stats.colaboradores.map((c: any) => ({
        nome: c.nome,
        total: c.total,
        distribuicao: c.distribuicao,
      })),
    });
    return buf;
  }

  // Resumo diário (compatível com antigo RelatoriosController.resumoDiario)
  async unidadeResumoDiario(unidadeId: string, data: string) {
    const unidadeRepo = new UnidadeRepository(this.ds);
    const avaliacaoRepo = new AvaliacaoRepository(this.ds);

    const histRepo = new HistoricoOcupacaoRepository(this.ds);

    const unidade = await unidadeRepo.obter(unidadeId);
    if (!unidade) throw new Error("Unidade não encontrada");

    const resumoAval = await avaliacaoRepo.resumoDiario({ data, unidadeId });
    let avalsDia = await avaliacaoRepo.listarPorDia({ data, unidadeId });

    // Fallback: se nenhuma avaliação retornou mas existem sessões ativas
    // (ex.: criadas após meia-noite UTC causando dataAplicacao diferente),
    // buscamos sessões ativas e filtramos por janela local do dia solicitado.
    if (avalsDia.length === 0) {
      const ZONE = "America/Sao_Paulo";
      const { DateTime } = require("luxon");
      const dayStart = DateTime.fromISO(data, { zone: ZONE }).startOf("day");
      const dayEnd = dayStart.endOf("day");
      const possiveis = await avaliacaoRepo.listarSessoesAtivasPorUnidade(
        unidadeId
      );
      avalsDia = possiveis.filter((a: any) => {
        if (!a.created_at) return false;
        const created = DateTime.fromJSDate(new Date(a.created_at), {
          zone: ZONE,
        });
        return created >= dayStart && created <= dayEnd;
      });
    }
    // Recalcula ocupUsada baseado no fallback (se aplicou)
    const ocupUsada = avalsDia.length || resumoAval.totalOcupados;
    const numeroLeitosTotal = unidade.leitos?.length ?? 0;

    // Verificar se é data atual ou histórica para determinar leitos inativos
    const { DateTime } = require("luxon");
    const hoje = DateTime.now().setZone("America/Sao_Paulo").toISODate();
    const ehDataAtual = data === hoje;

    let numeroLeitosInativos = 0;
    if (ehDataAtual) {
      // Para data atual, usar status atual dos leitos
      numeroLeitosInativos =
        unidade.leitos?.filter((l) => (l as any).status === StatusLeito.INATIVO)
          .length ?? 0;
    } else {
      // Para datas históricas, usar histórico de ocupação
      numeroLeitosInativos = await histRepo.contarLeitosInativosPorDia(
        data,
        unidadeId
      );
    }

    const numeroLeitosAtivos = numeroLeitosTotal - numeroLeitosInativos;
    const taxa = numeroLeitosTotal ? ocupUsada / numeroLeitosTotal : 0;

    // também pegar ocupação a partir do histórico de ocupação (para dias
    // anteriores ou quando consolidado no job). Usamos esse dado como
    // fonte adicional — o front pode escolher qual usar.
    const historicos = await histRepo.listarPorPeriodo(data, data, unidadeId);
    const leitosOcupadosHistorico = new Set<string>();
    for (const h of historicos) {
      if (h.leito && (h.leito as any).id)
        leitosOcupadosHistorico.add((h.leito as any).id);
    }
    const ocupHistorico = leitosOcupadosHistorico.size;
    const taxaHistorico = numeroLeitosTotal
      ? ocupHistorico / numeroLeitosTotal
      : 0;

    // breakdown por colaborador
    const porCol: Record<
      string,
      {
        colaboradorId: string;
        nome: string;
        total: number;
        distribuicao: Partial<Record<ClassificacaoCuidado, number>>;
      }
    > = {};
    for (const a of avalsDia) {
      const col = a.autor;
      if (!col) continue;
      if (!porCol[col.id]) {
        porCol[col.id] = {
          colaboradorId: col.id,
          nome: col.nome,
          total: 0,
          distribuicao: {},
        };
      }
      porCol[col.id].total += 1;
      const cls = a.classificacao as ClassificacaoCuidado;
      porCol[col.id].distribuicao[cls] =
        (porCol[col.id].distribuicao[cls] || 0) + 1;
    }

    // Zero-fill das classificações conhecidas
    const classificacoesBase = [
      "MINIMOS",
      "INTERMEDIARIOS",
      "ALTA_DEPENDENCIA",
      "SEMI_INTENSIVOS",
      "INTENSIVOS",
    ];
    const dist = { ...resumoAval.distribuicao } as any;
    for (const k of classificacoesBase) if (!dist[k]) dist[k] = 0;

    return {
      data,
      unidade: unidade.nome,
      numeroLeitosTotal,
      numeroLeitosInativos,
      numeroLeitos: numeroLeitosAtivos,
      metodo: (unidade as any).scpMetodoKey ?? null,
      ocupacao: {
        // ocupação diretamente derivada das avaliações do dia
        usadaAvaliacoes: ocupUsada,
        // ocupação derivada do histórico (consolidado) — útil para dias
        // anteriores onde as avaliações já foram consolidadas
        usadaHistorico: ocupHistorico,
      },
      taxaOcupacao: taxa,
      taxaOcupacaoHistorico: taxaHistorico,
      // quantidade total de avaliações no dia
      quantidadeAvaliacoes: ocupUsada,
      // distribuição por classificação (ex.: Minimos, Intermediarios...)
      distribuicao: dist,
      // nome alternativo explícito pedido pelo front: quantidade por classificação
      quantidadePorClassificacao: dist,
      colaboradores: Object.values(porCol),
    };
  }

  // Mensal (compatível com antigo RelatoriosController.mensal)
  async unidadeMensal(unidadeId: string, ano: number, mes: number) {
    const unidadeRepo = new UnidadeRepository(this.ds);
    const avaliacaoRepo = new AvaliacaoRepository(this.ds);

    const unidade = await unidadeRepo.obter(unidadeId);
    if (!unidade) throw new Error("Unidade não encontrada");

    const dias = new Date(Number(ano), Number(mes), 0).getDate();
    const serieAval: Array<{ data: string; ocupados: number }> = [];
    for (let d = 1; d <= dias; d++) {
      const data = `${ano}-${String(mes).padStart(2, "0")}-${String(d).padStart(
        2,
        "0"
      )}`;
      const lista = await avaliacaoRepo.listarPorDia({ data, unidadeId });
      serieAval.push({ data, ocupados: lista.length });
    }
    const numLeitos = unidade.leitos?.length ?? 0;

    const consAval = await avaliacaoRepo.consolidadoMensal(
      unidadeId,
      Number(ano),
      Number(mes)
    );

    const dataIni = new Date(Date.UTC(Number(ano), Number(mes) - 1, 1))
      .toISOString()
      .slice(0, 10);
    const dataFimExcl = new Date(Date.UTC(Number(ano), Number(mes), 1))
      .toISOString()
      .slice(0, 10);
    const avalsMes = await avaliacaoRepo.listarNoPeriodoComAutor(
      unidadeId,
      dataIni,
      dataFimExcl
    );
    const porColMes: Record<
      string,
      {
        colaboradorId: string;
        nome: string;
        total: number;
        distribuicao: Partial<Record<ClassificacaoCuidado, number>>;
      }
    > = {};
    for (const a of avalsMes) {
      const col = a.autor;
      if (!col) continue;
      if (!porColMes[col.id]) {
        porColMes[col.id] = {
          colaboradorId: col.id,
          nome: col.nome,
          total: 0,
          distribuicao: {},
        };
      }
      porColMes[col.id].total += 1;
      const cls = a.classificacao as ClassificacaoCuidado;
      porColMes[col.id].distribuicao[cls] =
        (porColMes[col.id].distribuicao[cls] || 0) + 1;
    }

    return {
      unidade: unidade.nome,
      ano: Number(ano),
      mes: Number(mes),
      numeroLeitos: numLeitos,
      ocupacaoMensal: {
        avaliacao: serieAval,
      },
      distribuicaoMensal: consAval.distribuicaoMensal,
      diasComMedicao: consAval.diasComMedicao,
      colaboradores: Object.values(porColMes),
    };
  }
}

function nextDateExclusive(d: string) {
  const dt = new Date(d);
  dt.setDate(dt.getDate() + 1);
  return dt.toISOString().slice(0, 10);
}
