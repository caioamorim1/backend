import { Request, Response } from "express";
import { RedeRepository } from "../repositories/redeRepository";

export class RedeController {
  constructor(private repo: RedeRepository) {}

  criar = async (req: Request, res: Response) => {
    try {
      const novo = await this.repo.criar(req.body);
      res.status(201).json(novo);
    } catch (error) {
      res.status(400).json({ error: "Erro ao criar rede" });
    }
  };

  listar = async (_: Request, res: Response) => {
    try {
      const items = await this.repo.buscarTodos();
      res.json(items);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar redes" });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const item = await this.repo.buscarPorId(id);
      if (!item) return res.status(404).json({ error: "Rede não encontrada" });
      res.json(item);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar rede" });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.atualizar(id, req.body);
      if (!sucesso)
        return res.status(404).json({ error: "Rede não encontrada" });
      const atualizado = await this.repo.buscarPorId(id);
      res.json(atualizado);
    } catch (error) {
      res.status(400).json({ error: "Erro ao atualizar rede" });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.deletar(id);
      if (!sucesso)
        return res.status(404).json({ error: "Rede não encontrada" });
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Erro ao deletar rede" });
    }
  };
}
