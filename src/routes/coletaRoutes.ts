import { Router } from "express";
import { DataSource } from "typeorm";
import { ColetaController } from "../controllers/coletaController";
import { ColetaRepository } from "../repositories/coletaRepository";
import { uploadColeta } from "../middlewares/multerColeta";

export function ColetaRoutes(ds: DataSource) {
  const router = Router();
  const repo = new ColetaRepository(ds);
  const ctrl = new ColetaController(repo);

  // Upload de fotos e criação da coleta
  router.post("/", uploadColeta.any(), ctrl.criar);
  router.get("/", ctrl.listar);
  router.get("/hospital/:hospitalId", ctrl.listarPorHospital);
  router.get("/:id", ctrl.buscarPorId);
  router.delete("/:id", ctrl.deletar);

  return router;
}
