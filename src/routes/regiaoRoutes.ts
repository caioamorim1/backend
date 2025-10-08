import { Router } from "express";
import { DataSource } from "typeorm";
import { RegiaoRepository } from "../repositories/regiaoRepository";
import { RegiaoController } from "../controllers/regiaoController";

export const RegiaoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new RegiaoRepository(ds);
  const ctrl = new RegiaoController(repo);

  r.post("/", ctrl.criar);
  r.get("/", ctrl.listar);
  r.get("/:id", ctrl.buscarPorId);
  r.put("/:id", ctrl.atualizar);
  r.delete("/:id", ctrl.deletar);

  return r;
};
