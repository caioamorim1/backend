import { Request, Response } from "express";
import { UnidadeRepository } from "../repositories/unidadeRepository";
import { AvaliacaoRepository } from "../repositories/avaliacaoRepository";
// InternaÃ§Ã£o removida na migraÃ§Ã£o. Exports relacionados a internaÃ§Ã£o foram desativados.
import { xlsResumoDiario, xlsMensal } from "../utils/exporters/excel";
import {
  pdfResumoDiario,
  pdfMensal,
  pdfConsolidadoMensal,
} from "../utils/exporters/pdf";
import { DataSource } from "typeorm";

export class ExportController {
  constructor(private ds: DataSource) {}

  /** GET /export/relatorios/resumo-diario.xlsx?unidadeId=&data=YYYY-MM-DD */
  resumoDiarioXlsx = async (req: Request, res: Response) => {
    const { unidadeId, data } = req.query as any;
    const unidadeRepo = new UnidadeRepository(this.ds);
    const avaliacaoRepo = new AvaliacaoRepository(this.ds);
    // InternaÃ§Ã£o desativada na migraÃ§Ã£o

    const unidade = await unidadeRepo.obter(unidadeId);
    if (!unidade) return res.status(404).end();

    const resumoAval = await avaliacaoRepo.resumoDiario({ data, unidadeId });
    const avalsDia = await avaliacaoRepo.listarPorDia({ data, unidadeId });
    const ocupUsada = resumoAval.totalOcupados;
    const numLeitos = unidade.leitos?.length ?? 0;
    const taxa = numLeitos ? ocupUsada / numLeitos : 0;

    // colaboradores breakdown
    const porCol: Record<
      string,
      {
        colaboradorId: string;
        nome: string;
        total: number;
        distribuicao: Record<string, number>;
      }
    > = {};
    for (const a of avalsDia) {
      const col = a.autor;
      if (!col) continue;
      if (!porCol[col.id])
        porCol[col.id] = {
          colaboradorId: col.id,
          nome: col.nome,
          total: 0,
          distribuicao: {},
        };
      porCol[col.id].total += 1;
      porCol[col.id].distribuicao[a.classificacao] =
        (porCol[col.id].distribuicao[a.classificacao] || 0) + 1;
    }

    const buf = await xlsResumoDiario({
      data,
      unidade: unidade.nome,
      numeroLeitos: numLeitos,
      ocupacao: {
        usada: ocupUsada,
      },
      taxaOcupacao: taxa,
      distribuicao: resumoAval.distribuicao as any,
      colaboradores: Object.values(porCol),
    });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="resumo_diario_${unidadeId}_${data}.xlsx"`
    );
    return res.send(Buffer.from(buf));
  };

  /** GET /export/relatorios/mensal.xlsx?unidadeId=&ano=&mes= */
  mensalXlsx = async (req: Request, res: Response) => {
    const { unidadeId, ano, mes } = req.query as any;
    const unidadeRepo = new UnidadeRepository(this.ds);
    const avaliacaoRepo = new AvaliacaoRepository(this.ds);
    // InternaÃ§Ã£o desativada na migraÃ§Ã£o

    const unidade = await unidadeRepo.obter(unidadeId);
    if (!unidade) return res.status(404).end();
    const dias = new Date(Number(ano), Number(mes), 0).getDate();
    const serieAval: Array<{ data: string; ocupados: number }> = [];
    let totalOcupadosMes = 0;
    let diasComDados = 0;

    for (let d = 1; d <= dias; d++) {
      const data = `${ano}-${String(mes).padStart(2, "0")}-${String(d).padStart(
        2,
        "0"
      )}`;

      // CORREÇÃO: Usar resumoDiario que retorna dados consolidados corretos
      const resumoDia = await avaliacaoRepo.resumoDiario({ data, unidadeId });
      const ocupadosNoDia = resumoDia.totalOcupados || 0;

      serieAval.push({ data, ocupados: ocupadosNoDia });

      // Acumular para calcular média
      if (ocupadosNoDia > 0) {
        totalOcupadosMes += ocupadosNoDia;
        diasComDados++;
      }
    }
    const numLeitos = unidade.leitos?.length ?? 0;
    const consAval = await avaliacaoRepo.consolidadoMensal(
      unidadeId,
      Number(ano),
      Number(mes)
    );

    // CORREÇÃO: Calcular valores corretos se os do consolidado estão incorretos
    const mediaOcupadosDia =
      diasComDados > 0 ? totalOcupadosMes / diasComDados : 0;
    const taxaOcupacaoMedia = numLeitos > 0 ? mediaOcupadosDia / numLeitos : 0;

    // colaboradores no mÃªs
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
        distribuicao: Record<string, number>;
      }
    > = {};
    for (const a of avalsMes) {
      const col = a.autor;
      if (!col) continue;
      if (!porColMes[col.id])
        porColMes[col.id] = {
          colaboradorId: col.id,
          nome: col.nome,
          total: 0,
          distribuicao: {},
        };
      porColMes[col.id].total += 1;
      porColMes[col.id].distribuicao[a.classificacao] =
        (porColMes[col.id].distribuicao[a.classificacao] || 0) + 1;
    }

    const buf = await xlsMensal({
      ano: Number(ano),
      mes: Number(mes),
      numeroLeitos: numLeitos,
      ocupacaoMensal: {
        avaliacao: serieAval,
        // CORREÇÃO: Usar valores calculados localmente
        mediaOcupadosDia: mediaOcupadosDia,
        taxaOcupacaoMedia: taxaOcupacaoMedia,
      },
      distribuicaoMensal: consAval.distribuicaoMensal as any,
      colaboradores: Object.values(porColMes),
    });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="mensal_${unidadeId}_${ano}-${mes}.xlsx"`
    );
    return res.send(Buffer.from(buf));
  };

  /** GET /export/relatorios/resumo-diario.pdf?unidadeId=&data= */
  resumoDiarioPdf = async (req: Request, res: Response) => {
    const { unidadeId, data } = req.query as any;
    const unidadeRepo = new UnidadeRepository(this.ds);
    const avaliacaoRepo = new AvaliacaoRepository(this.ds);
    // InternaÃ§Ã£o desativada na migraÃ§Ã£o

    const unidade = await unidadeRepo.obter(unidadeId);
    if (!unidade) return res.status(404).end();

    const resumoAval = await avaliacaoRepo.resumoDiario({ data, unidadeId });
    const avalsDia = await avaliacaoRepo.listarPorDia({ data, unidadeId });
    const ocupUsada = resumoAval.totalOcupados;
    const numLeitos = unidade.leitos?.length ?? 0;
    const taxa = numLeitos ? ocupUsada / numLeitos : 0;

    const porCol: Record<
      string,
      {
        colaboradorId: string;
        nome: string;
        total: number;
        distribuicao: Record<string, number>;
      }
    > = {};
    for (const a of avalsDia) {
      const col = a.autor;
      if (!col) continue;
      if (!porCol[col.id])
        porCol[col.id] = {
          colaboradorId: col.id,
          nome: col.nome,
          total: 0,
          distribuicao: {},
        };
      porCol[col.id].total += 1;
      porCol[col.id].distribuicao[a.classificacao] =
        (porCol[col.id].distribuicao[a.classificacao] || 0) + 1;
    }

    const pdf = await pdfResumoDiario({
      data,
      unidade: unidade.nome,
      numeroLeitos: numLeitos,
      ocupacao: {
        usada: ocupUsada,
      },
      taxaOcupacao: taxa,
      distribuicao: resumoAval.distribuicao as any,
    });

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="resumo_diario_${unidadeId}_${data}.pdf"`
    );
    return res.send(pdf);
  };

  /** GET /export/relatorios/mensal.pdf?unidadeId=&ano=&mes= */
  mensalPdf = async (req: Request, res: Response) => {
    const { unidadeId, ano, mes } = req.query as any;
    const unidadeRepo = new UnidadeRepository(this.ds);
    const avaliacaoRepo = new AvaliacaoRepository(this.ds);
    const unidade = await unidadeRepo.obter(unidadeId);
    if (!unidade) return res.status(404).end();

    const dias = new Date(Number(ano), Number(mes), 0).getDate();
    const serieAval: Array<{ data: string; ocupados: number }> = [];
    let totalOcupadosMes = 0;
    let diasComDados = 0;

    for (let d = 1; d <= dias; d++) {
      const data = `${ano}-${String(mes).padStart(2, "0")}-${String(d).padStart(
        2,
        "0"
      )}`;

      // CORREÇÃO: Usar resumoDiario que retorna dados consolidados corretos
      const resumoDia = await avaliacaoRepo.resumoDiario({ data, unidadeId });
      const ocupadosNoDia = resumoDia.totalOcupados || 0;

      serieAval.push({ data, ocupados: ocupadosNoDia });

      // Acumular para calcular média
      if (ocupadosNoDia > 0) {
        totalOcupadosMes += ocupadosNoDia;
        diasComDados++;
      }
    }
    const numLeitos = unidade.leitos?.length ?? 0;
    const consAval = await avaliacaoRepo.consolidadoMensal(
      unidadeId,
      Number(ano),
      Number(mes)
    );

    // CORREÇÃO: Calcular valores corretos se os do consolidado estão incorretos
    const mediaOcupadosDia =
      diasComDados > 0 ? totalOcupadosMes / diasComDados : 0;
    const taxaOcupacaoMedia = numLeitos > 0 ? mediaOcupadosDia / numLeitos : 0;

    // colaboradores no mÃªs
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
        distribuicao: Record<string, number>;
      }
    > = {};
    for (const a of avalsMes) {
      const col = a.autor;
      if (!col) continue;
      if (!porColMes[col.id])
        porColMes[col.id] = {
          colaboradorId: col.id,
          nome: col.nome,
          total: 0,
          distribuicao: {},
        };
      porColMes[col.id].total += 1;
      porColMes[col.id].distribuicao[a.classificacao] =
        (porColMes[col.id].distribuicao[a.classificacao] || 0) + 1;
    }

    const pdf = await pdfMensal({
      unidade: unidade.nome,
      ano: Number(ano),
      mes: Number(mes),
      numeroLeitos: numLeitos,
      ocupacaoMensal: {
        avaliacao: serieAval,
        // CORREÇÃO: Usar valores calculados localmente
        mediaOcupadosDia: mediaOcupadosDia,
        taxaOcupacaoMedia: taxaOcupacaoMedia,
      },
      distribuicaoMensal: consAval.distribuicaoMensal as any,
      colaboradores: Object.values(porColMes),
    });

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="mensal_${unidadeId}_${ano}-${mes}.pdf"`
    );
    return res.send(pdf);
  };

  /** GET /export/relatorios/consolidado-mensal.pdf?unidadeId=&dataInicial=YYYY-MM&dataFinal=YYYY-MM */
  consolidadoMensalPdf = async (req: Request, res: Response) => {
    try {
      const { unidadeId, dataInicial, dataFinal } = req.query as {
        unidadeId?: string;
        dataInicial?: string;
        dataFinal?: string;
      };

      // Validações
      if (!unidadeId || !dataInicial || !dataFinal) {
        return res.status(400).json({
          mensagem: "unidadeId, dataInicial e dataFinal são obrigatórios",
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

      // Buscar dados da unidade
      const unidadeRepo = new UnidadeRepository(this.ds);
      const unidade = await unidadeRepo.obter(unidadeId);
      if (!unidade) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      // Usar a lógica do histórico mensal diretamente
      const avaliacaoRepo = new AvaliacaoRepository(this.ds);
      const historicoMensal = [];

      // Iterar por cada mês no intervalo
      let anoAtual = anoInicial;
      let mesAtual = mesInicial;

      while (
        anoAtual < anoFinal ||
        (anoAtual === anoFinal && mesAtual <= mesFinal)
      ) {
        const mesAno = `${String(mesAtual).padStart(2, "0")}/${anoAtual}`;
        const diasNoMes = new Date(anoAtual, mesAtual, 0).getDate();

        let cuidadosMinimos = 0,
          cuidadosIntermediarios = 0,
          cuidadosAltaDependencia = 0;
        let cuidadosSemiIntensivos = 0,
          cuidadosIntensivos = 0;
        let leitosOperacionaisMes = 0,
          totalLeitosMes = 0,
          leitosOcupadosMes = 0;

        // Processar cada dia do mês
        for (let dia = 1; dia <= diasNoMes; dia++) {
          const dataISO = `${anoAtual}-${String(mesAtual).padStart(
            2,
            "0"
          )}-${String(dia).padStart(2, "0")}`;

          const avalsDia = await avaliacaoRepo.listarPorDia({
            data: dataISO,
            unidadeId: unidadeId,
          });

          // Usar a mesma lógica corrigida
          const totalLeitosDia = unidade.leitos?.length || 0;
          const leitosOcupadosDia = avalsDia.length;

          totalLeitosMes += totalLeitosDia;
          leitosOperacionaisMes += totalLeitosDia; // Simplificado - todos operacionais
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

        const taxaOcupacaoMedia =
          totalLeitosMes > 0
            ? Math.round((leitosOcupadosMes / totalLeitosMes) * 100)
            : 0;

        historicoMensal.push({
          mesAno,
          cuidadosMinimos,
          cuidadosIntermediarios,
          cuidadosAltaDependencia,
          cuidadosSemiIntensivos,
          cuidadosIntensivos,
          somaLeitos: totalLeitosMes,
          leitosOperacionais: leitosOperacionaisMes,
          percentualOcupacao: taxaOcupacaoMedia,
        });

        // Próximo mês
        mesAtual++;
        if (mesAtual > 12) {
          mesAtual = 1;
          anoAtual++;
        }
      }

      // Gerar PDF
      const pdf = await pdfConsolidadoMensal({
        unidade: unidade.nome,
        dataInicial,
        dataFinal,
        historicoMensal,
      });

      res.setHeader("Content-Type", "application/pdf");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="relatorio_consolidado_mensal_${unidadeId}_${dataInicial}_${dataFinal}.pdf"`
      );
      return res.send(pdf);
    } catch (error) {
      console.error("Erro ao gerar relatório consolidado mensal:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: (error as Error).message,
      });
    }
  };
}
