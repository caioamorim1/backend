import { Router } from "express";
import { DataSource } from "typeorm";
import { LeitoRepository } from "../repositories/leitoRepository";
import { LeitoController } from "../controllers/leitoController";

export const LeitoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new LeitoRepository(ds);
  const ctrl = new LeitoController(repo, ds);

  r.post("/", ctrl.criar);
  r.get("/", ctrl.listar);
  r.patch("/:id", ctrl.atualizar);
  r.patch("/:id/status", ctrl.atualizarStatus);
  r.delete("/:id", ctrl.deletar);

  return r;
};
