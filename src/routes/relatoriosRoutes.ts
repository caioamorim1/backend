import { Router } from "express";
import { DataSource } from "typeorm";
import { RelatoriosController } from "../services/relatoriosService";
import { UnidadeRepository } from "../repositories/unidadeRepository";
import { AvaliacaoRepository } from "../repositories/avaliacaoRepository";
import { StatisticsService } from "../services/statisticsService";

export const RelatoriosRoutes = (ds: DataSource): Router => {
  const r = Router();
  const svc = new StatisticsService(ds);
  const ctrl = new RelatoriosController(svc);
  r.get("/resumo-diario", ctrl.resumoDiario);

  r.get("/mensal", ctrl.mensal);

  return r;
};
