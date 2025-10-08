import { Request, Response } from "express";
import { UnidadeRepository } from "../repositories/unidadeRepository";
import { AvaliacaoRepository } from "../repositories/avaliacaoRepository";
import { StatisticsService } from "./statisticsService";

export class RelatoriosController {
  constructor(private svc: StatisticsService) {}

  resumoDiario = async (req: Request, res: Response) => {
    try {
      const { data, unidadeId } = req.query as {
        data: string;
        unidadeId: string;
      };
      if (!data || !unidadeId)
        return res
          .status(400)
          .json({ mensagem: "Parâmetros data e unidadeId são obrigatórios" });
      const json = await this.svc.unidadeResumoDiario(unidadeId, data);
      return res.json(json);
    } catch (e) {
      return res.status(400).json({ error: (e as Error).message });
    }
  };

  mensal = async (req: Request, res: Response) => {
    try {
      const { unidadeId, ano, mes } = req.query as {
        unidadeId: string;
        ano: string;
        mes: string;
      };
      if (!unidadeId || !ano || !mes)
        return res.status(400).json({
          mensagem: "Parâmetros unidadeId, ano e mes são obrigatórios",
        });
      const json = await this.svc.unidadeMensal(
        unidadeId,
        Number(ano),
        Number(mes)
      );
      return res.json(json);
    } catch (e) {
      return res.status(400).json({ error: (e as Error).message });
    }
  };
}
