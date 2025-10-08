import { Router } from "express";
import { DataSource } from "typeorm";
import { CargoRepository } from "../repositories/cargoRepository";
import { CargoHospitalController } from "../controllers/cargoHospitalController";

export const CargoHospitalRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new CargoRepository(ds);
  const ctrl = new CargoHospitalController(repo);

  // Rotas para cargos específicos de hospital

  r.post("/:hospitalId/cargos", ctrl.criar);
  r.get("/:hospitalId/cargos", ctrl.listar);
  r.get("/:hospitalId/cargos/:cargoId", ctrl.obter);
  r.patch("/:hospitalId/cargos/:cargoId", ctrl.atualizar);
  r.delete("/:hospitalId/cargos/:cargoId", ctrl.deletar);

  return r;
};
