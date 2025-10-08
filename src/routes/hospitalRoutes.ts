import { Router } from "express";
import { HospitalRepository } from "../repositories/hospitalRepository";
import { HospitalController } from "../controllers/hospitalController";
import { DataSource } from "typeorm";

export const HospitalRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new HospitalRepository(ds);
  const ctrl = new HospitalController(repo);

  // Route: POST /hospitais
  r.post("/", ctrl.criar);

  // Route: GET /hospitais
  r.get("/", ctrl.listar);

  // Route: GET /hospitais/:id
  r.get("/:id", ctrl.buscarPorId);

  // Route: PUT /hospitais/:id
  r.put("/:id", ctrl.atualizar);

  // Route: DELETE /hospitais/:id
  r.delete("/:id", ctrl.deletar);

  return r;
};
