import { Request, Response } from "express";
import { GrupoRepository } from "../repositories/grupoRepository";

export class GrupoController {
  constructor(private repo: GrupoRepository) {}

  criar = async (req: Request, res: Response) => {
    try {
      const novo = await this.repo.criar(req.body);
      res.status(201).json(novo);
    } catch (error) {
      res.status(400).json({ error: "Erro ao criar grupo" });
    }
  };

  listar = async (_: Request, res: Response) => {
    try {
      const items = await this.repo.buscarTodos();
      res.json(items);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar grupos" });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const item = await this.repo.buscarPorId(id);
      if (!item) return res.status(404).json({ error: "Grupo não encontrado" });
      res.json(item);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar grupo" });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const atualizado = await this.repo.atualizar(id, req.body);
      if (!atualizado)
        return res.status(404).json({ error: "Grupo não encontrado" });
      res.json(atualizado);
    } catch (error) {
      if (error instanceof Error) {
        if (error.message === "Grupo não encontrado") {
          return res.status(404).json({ error: error.message });
        }
        if (error.message === "Rede não encontrada") {
          return res.status(400).json({ error: error.message });
        }
      }
      res.status(500).json({ error: "Erro ao atualizar grupo" });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.deletar(id);
      if (!sucesso)
        return res.status(404).json({ error: "Grupo não encontrado" });
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Erro ao deletar grupo" });
    }
  };
}
