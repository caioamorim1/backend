import { Router } from "express";
import { DataSource } from "typeorm";
import { SitioFuncionalRepository } from "../repositories/sitioFuncionalRepository";
import { SitioFuncionalController } from "../controllers/sitioFuncionalController";
import { CargoSitioRepository } from "../repositories/cargoSitioRepository";
import { CargoSitioController } from "../controllers/cargoSitioController";

export const SitioPosicaoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const sitioRepo = new SitioFuncionalRepository(ds);
  const sitioCtrl = new SitioFuncionalController(sitioRepo);
  const cargoSitioRepo = new CargoSitioRepository(ds);
  const cargoSitioCtrl = new CargoSitioController(cargoSitioRepo);

  // Sítios Funcionais - CRUD completo
  r.get("/sitios-funcionais", sitioCtrl.listar);
  // Route: POST /sitios-funcionais
  r.post("/sitios-funcionais", sitioCtrl.criar);
  r.get("/sitios-funcionais/:id", sitioCtrl.obter);
  // Listar posicoes de um sitio
  r.get("/sitios-funcionais/:id/posicoes", sitioCtrl.posicoes);
  r.put("/sitios-funcionais/:id", sitioCtrl.atualizar);
  r.delete("/sitios-funcionais/:id", sitioCtrl.deletar);

  // Sítios por unidade (manter compatibilidade)
  r.get("/unidades-nao-internacao/:id/sitios", sitioCtrl.listarPorUnidade);

  // Cargos atribuídos a um sítio
  r.get("/sitios-funcionais/:id/cargos", cargoSitioCtrl.listarPorSitio);
  r.post("/sitios-funcionais/:id/cargos", cargoSitioCtrl.criar);
  r.get("/sitios-funcionais/cargos/:id", cargoSitioCtrl.obter);
  r.patch("/sitios-funcionais/cargos/:id", cargoSitioCtrl.atualizar);
  r.delete("/sitios-funcionais/cargos/:id", cargoSitioCtrl.deletar);

  return r;
};
