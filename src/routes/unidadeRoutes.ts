import { Router } from "express";
import { UnidadeRepository } from "../repositories/unidadeRepository";
import { UnidadeController } from "../controllers/unidadeController";

import { DataSource } from "typeorm";
import { post } from "axios";

export const UnidadeRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new UnidadeRepository(ds);
  const ctrl = new UnidadeController(repo, ds);

  // Route: POST /unidades
  r.post("/", ctrl.criar);
  // Route: GET /unidades
  r.get("/", ctrl.listar);
  r.put("/:id", ctrl.atualizar);
  r.delete("/:id", ctrl.deletar);

  // Route: GET /unidades/:id
  r.get("/:id", ctrl.obter);

  // Route: GET /unidades/:id/estatisticas-consolidadas
  r.get("/:id/estatisticas-consolidadas", ctrl.estatisticasConsolidadas);

  // Route: GET /unidades/:id/resumo-mensal
  r.get("/:id/resumo-mensal", ctrl.resumoMensal);
  // Route: GET /unidades/:id/historico-mensal
  r.get("/:id/historico-mensal", ctrl.historicoMensal);

  // Rotas de dimensionamento

  return r;
};
