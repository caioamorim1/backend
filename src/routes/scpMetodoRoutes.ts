import { Router } from "express";
import { DataSource } from "typeorm";
import { ScpMetodoRepository } from "../repositories/scpMetodoRepository";
import { ScpMetodoController } from "../controllers/scpMetodoController";

export const ScpMetodoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const ctrl = new ScpMetodoController(new ScpMetodoRepository(ds));

  r.get("/", ctrl.list);
  r.get("/key/:key", ctrl.getByKey);
  r.post("/", ctrl.create);
  r.put("/:id", ctrl.update);
  r.delete("/:id", ctrl.remove);

  // opcional: cria/atualiza embutidos do código
  r.post("/seed/builtin", ctrl.seed);

  // rota por id
  r.get("/:id", ctrl.get);

  return r;
};
