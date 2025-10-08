import { Router } from "express";
import { DataSource } from "typeorm";
import { ExportController } from "../controllers/exportController";

export const ExportRoutes = (ds: DataSource): Router => {
  const r = Router();
  const ctrl = new ExportController(ds);

  r.get("/relatorios/resumo-diario.xlsx", ctrl.resumoDiarioXlsx);

  r.get("/relatorios/mensal.xlsx", ctrl.mensalXlsx);
  r.get("/relatorios/mensal.pdf", ctrl.mensalPdf);
  r.get("/relatorios/resumo-diario.pdf", ctrl.resumoDiarioPdf);
  r.get("/relatorios/consolidado-mensal.pdf", ctrl.consolidadoMensalPdf);

  return r;
};
