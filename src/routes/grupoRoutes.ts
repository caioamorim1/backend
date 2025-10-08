import { Router } from "express";
import { DataSource } from "typeorm";
import { GrupoRepository } from "../repositories/grupoRepository";
import { GrupoController } from "../controllers/grupoController";

export const GrupoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new GrupoRepository(ds);
  const ctrl = new GrupoController(repo);

  r.post("/", ctrl.criar);
  r.get("/", ctrl.listar);
  r.get("/:id", ctrl.buscarPorId);
  r.put("/:id", ctrl.atualizar);

  r.delete("/:id", ctrl.deletar);

  return r;
};
