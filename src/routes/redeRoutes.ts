import { Router } from "express";
import { DataSource } from "typeorm";
import { RedeRepository } from "../repositories/redeRepository";
import { RedeController } from "../controllers/redeController";

export const RedeRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new RedeRepository(ds);
  const ctrl = new RedeController(repo);

  r.post("/", ctrl.criar);
  r.get("/", ctrl.listar);
  r.get("/:id", ctrl.buscarPorId);
  r.put("/:id", ctrl.atualizar);
  r.delete("/:id", ctrl.deletar);

  return r;
};
