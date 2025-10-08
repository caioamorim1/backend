import { Router } from "express";
import { DataSource } from "typeorm";
import { AdminRepository } from "../repositories/adminRepository";

export const AdminRoutes = (dataSource: DataSource) => {
  const router = Router();

  // Route: POST /admin/criar - cria um novo administrador
  router.post("/criar", async (req, res) => {
    try {
      const repo = new AdminRepository(dataSource);
      const created = await repo.criar(req.body);
      return res.status(201).json(created);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return res.status(500).json({ mensagem: "Erro interno", erro: msg });
    }
  });

  return router;
};
// If controller handled response, return; otherwise attempt to send created admin
