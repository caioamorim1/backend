import { Router } from "express";
import { DataSource } from "typeorm";
import { EstatisticasController } from "../controllers/estatisticasController";
import { authMiddleware } from "../middlewares/authMiddleware";

export const EstatisticasRoutes = (dataSource: DataSource) => {
  const router = Router();
  const estatisticasController = new EstatisticasController(dataSource);

  // Middleware de autenticação para todas as rotas
  router.use(authMiddleware);

  router.get(
    "/unidades-nao-internacao/:id/relatorio-mensal",
    estatisticasController.relatorioMensalUnidade.bind(estatisticasController)
  );

  router.get(
    "/sitios/:id/estatisticas",
    estatisticasController.estatisticasSitio.bind(estatisticasController)
  );

  return router;
};
