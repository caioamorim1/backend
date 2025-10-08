import { Router } from "express";
import { DataSource } from "typeorm";
import { DimensionamentoService } from "../services/dimensionamentoService";
import { DimensionamentoController } from "../controllers/dimensionamentoController";

export const DimensionamentoRoutes = (ds: DataSource): Router => {
  const router = Router();
  const service = new DimensionamentoService(ds);
  const controller = new DimensionamentoController(service);

  // Rota para obter a análise de uma unidade de INTERNAÇÃO
  router.get("/internacao/:unidadeId", controller.analiseInternacao);

  // Rota para obter a análise de uma unidade de NÃO INTERNAÇÃO
  router.get("/nao-internacao/:unidadeId", controller.analiseNaoInternacao);

  return router;
};
