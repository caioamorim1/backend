import { Router } from "express";
import { QuestionarioController } from "../controllers/questionarioController";
import { DataSource } from "typeorm";
import { QuestionarioRepository } from "../repositories/questionarioRepository";

export const QuestionarioRoutes = (ds: DataSource): Router => {
  const router = Router();
  const repo = new QuestionarioRepository(ds);
  const ctrl = new QuestionarioController(repo);

  router.post("/", ctrl.criar);
  router.get("/", ctrl.listarTodos);
  router.get("/:id", ctrl.buscarPorId);
  router.put("/:id", ctrl.atualizar);
  router.delete("/:id", ctrl.excluir);

  return router;
};
