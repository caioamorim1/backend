import { Router } from "express";
import { DataSource } from "typeorm";
import { ColaboradorRepository } from "../repositories/colaboradorRepository";
import { ColaboradorController } from "../controllers/colaboradorController";
import { AuthService } from "../services/authService";

export const ColaboradorRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new ColaboradorRepository(ds);
  const auth = new AuthService(ds);
  const ctrl = new ColaboradorController(repo, auth);

  r.get("/admin", ctrl.listarAdmins);
  r.post("/admin", ctrl.criarAdmin);
  r.delete("/admin/:id", ctrl.deletarAdmin);
  // Route: POST /colaboradores
  r.post("/", ctrl.criar);
  r.get("/", ctrl.listar); // ?unidadeId=...
  r.get("/:id", ctrl.obter);
  r.patch("/:id", ctrl.atualizar);
  r.delete("/:id", ctrl.deletar);

  r.patch("/:id/senha", ctrl.changePassword);

  return r;
};
