import { Router } from "express";
import { DataSource } from "typeorm";
import { ParametrosNaoInternacaoRepository } from "../repositories/parametrosNaoInternacaoRepository";
import { ParametrosNaoInternacaoController } from "../controllers/parametrosNaoInternacaoController";

export const ParametrosNaoInternacaoRoutes = (ds: DataSource): Router => {
  const router = Router();
  const controller = new ParametrosNaoInternacaoController(
    new ParametrosNaoInternacaoRepository(ds)
  );

  router.get("/nao-internacao/:unidadeId", controller.obter);
  router.post("/nao-internacao/:unidadeId", controller.salvar);
  router.delete("/nao-internacao/:unidadeId", controller.deletar);

  return router;
};
