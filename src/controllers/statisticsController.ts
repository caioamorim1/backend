import { Request, Response } from "express";
import { StatisticsService } from "../services/statisticsService";

export class StatisticsController {
  constructor(private service: StatisticsService) {}

  unidadeJson = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const { dataIni, dataFim } = req.query as {
        dataIni?: string;
        dataFim?: string;
      };
      const stats = await this.service.unidadeStats(id, dataIni, dataFim);
      return res.json(stats);
    } catch (e) {
      return res.status(400).json({ error: (e as Error).message });
    }
  };

  unidadePdf = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const { data, dataIni, dataFim } = req.query as any;

      const { DateTime } = require("luxon");
      const today =
        DateTime.now().setZone("America/Sao_Paulo").toISODate() ||
        new Date().toISOString().slice(0, 10);
      const ini = (data ?? dataIni ?? today).slice(0, 10);
      const fim = (data ?? dataFim ?? dataIni ?? today).slice(0, 10);

      // chama o service
      const pdfBuffer = await this.service.exportUnidadePdf(id, ini, fim);

      res.setHeader("Content-Type", "application/pdf");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename=relatorio_unidade_${id}_${ini}_a_${fim}.pdf`
      );
      return res.send(pdfBuffer); // <-- aqui usa pdfBuffer
    } catch (err) {
      const message = err instanceof Error ? err.message : "Erro ao gerar PDF";
      return res.status(500).json({ error: message });
    }
  };

  hospitalJson = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const { dataIni, dataFim } = req.query as {
        dataIni?: string;
        dataFim?: string;
      };
      const stats = await this.service.hospitalStats(id, dataIni, dataFim);
      return res.json(stats);
    } catch (e) {
      return res.status(400).json({ error: (e as Error).message });
    }
  };

  // no PDF export for hospital-level yet (could combine unit PDFs)
  hospitalPdf = async (req: Request, res: Response) => {
    return res
      .status(501)
      .json({ error: "Export PDF por hospital não implementado" });
  };
}
