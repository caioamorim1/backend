import { Router } from "express";
import { DataSource } from "typeorm";
import { CargoRepository } from "../repositories/cargoRepository";
import { CargoController } from "../controllers/cargoController";

export const CargoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new CargoRepository(ds);
  const ctrl = new CargoController(repo);

  r.post("/", ctrl.criar);
  r.get("/", ctrl.listar);
  r.get("/:id", ctrl.obter);
  r.patch("/:id", ctrl.atualizar);
  r.delete("/:id", ctrl.deletar);

  return r;
};
