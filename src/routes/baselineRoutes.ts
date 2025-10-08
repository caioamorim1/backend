import { Router } from "express";
import { DataSource } from "typeorm";
import { BaselineRepository } from "../repositories/baselineRepository";
import { BaselineController } from "../controllers/baselineController";

export const BaselineRoutes = (ds: DataSource) => {
  const repo = new BaselineRepository(ds);
  const controller = new BaselineController(repo);
  const router = Router();

  router.post("/", controller.criar);
  router.put("/:id", controller.atualizar);
  router.get("/", controller.listar);
  router.get("/:id", controller.buscarPorId);
  router.delete("/:id", controller.deletar);
  router.patch("/:id/setores/:setorNome/status", controller.alterarStatusSetor);

  return router;
};
