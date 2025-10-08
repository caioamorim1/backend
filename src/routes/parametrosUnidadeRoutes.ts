import { Router } from "express";
import { DataSource } from "typeorm";
import { ParametrosUnidadeRepository } from "../repositories/parametrosUnidadeRepository";
import { ParametrosUnidadeController } from "../controllers/parametrosUnidadeController";

export const ParametrosUnidadeRoutes = (ds: DataSource): Router => {
  const r = Router();
  const ctrl = new ParametrosUnidadeController(
    new ParametrosUnidadeRepository(ds)
  );

  // GET /parametros/unidade/:unidadeId
  r.get("/unidade/:unidadeId", ctrl.obter);

  // POST /parametros/unidade/:unidadeId
  r.post("/unidade/:unidadeId", ctrl.create);

  // DELETE /parametros/unidade/:unidadeId
  r.delete("/unidade/:unidadeId", ctrl.deletar);

  return r;
};
