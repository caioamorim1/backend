import { Request, Response } from "express";
import { RegiaoRepository } from "../repositories/regiaoRepository";

export class RegiaoController {
  constructor(private repo: RegiaoRepository) {}

  criar = async (req: Request, res: Response) => {
    try {
      const novo = await this.repo.criar(req.body);
      res.status(201).json(novo);
    } catch (error) {
      res.status(400).json({ error: "Erro ao criar região" });
    }
  };

  listar = async (_: Request, res: Response) => {
    try {
      const items = await this.repo.buscarTodos();
      res.json(items);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar regiões" });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const item = await this.repo.buscarPorId(id);
      if (!item)
        return res.status(404).json({ error: "Região não encontrada" });
      res.json(item);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar região" });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.atualizar(id, req.body);
      if (!sucesso)
        return res.status(404).json({ error: "Região não encontrada" });
      const atualizado = await this.repo.buscarPorId(id);
      res.json(atualizado);
    } catch (error) {
      res.status(400).json({ error: "Erro ao atualizar região" });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.deletar(id);
      if (!sucesso)
        return res.status(404).json({ error: "Região não encontrada" });
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Erro ao deletar região" });
    }
  };
}
