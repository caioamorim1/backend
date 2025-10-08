import { Request, Response } from "express";
import { HistoricoOcupacaoRepository } from "../repositories/historicoOcupacaoRepository";

export class HistoricoOcupacaoController {
  constructor(private repo: HistoricoOcupacaoRepository) {}

  listarPorDia = async (req: Request, res: Response) => {
    const { data, unidadeId } = req.query as {
      data: string;
      unidadeId?: string;
    };
    if (!data)
      return res
        .status(400)
        .json({ error: "query param 'data' required (yyyy-mm-dd)" });
    try {
      const rows = await this.repo.listarPorDia(data, unidadeId);
      return res.json(rows);
    } catch (e) {
      console.error(e);
      return res.status(500).json({ error: String(e) });
    }
  };

  listarPorPeriodo = async (req: Request, res: Response) => {
    const { dataIni, dataFim, unidadeId, hospitalId } = req.query as {
      dataIni: string;
      dataFim: string;
      unidadeId?: string;
      hospitalId?: string;
    };

    if (!dataIni || !dataFim)
      return res
        .status(400)
        .json({ error: "query params 'dataIni' and 'dataFim' required" });

    try {
      const rows = await this.repo.listarPorPeriodo(
        dataIni,
        dataFim,
        unidadeId,
        hospitalId
      );
      return res.json(rows);
    } catch (e) {
      console.error(e);
      return res.status(500).json({ error: String(e) });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    const { id } = req.params;
    try {
      const row = await this.repo.buscarPorId(id);
      if (!row) return res.status(404).json({ error: "not found" });
      return res.json(row);
    } catch (e) {
      console.error(e);
      return res.status(500).json({ error: String(e) });
    }
  };
}
