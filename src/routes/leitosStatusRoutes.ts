import { Router } from "express";
import { DataSource } from "typeorm";
import { LeitosStatusService } from "../services/leitosStatusService";
import { LeitosStatusController } from "../controllers/leitosStatusController";

export const LeitosStatusRoutes = (ds: DataSource): Router => {
  const router = Router();
  const service = new LeitosStatusService(ds);
  const ctrl = new LeitosStatusController(service);

  // PUT /leitos-status/unidade/:unidadeId
  router.put("/unidade/:unidadeId", ctrl.atualizarUnidade);

  // PUT /leitos-status/hospital/:hospitalId
  router.put("/hospital/:hospitalId", ctrl.atualizarHospital);

  // PUT /leitos-status/sync
  router.put("/sync", ctrl.atualizarTodas);

  return router;
};
