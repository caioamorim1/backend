import { Request, Response } from "express";
import { HospitalRepository } from "../repositories/hospitalRepository";

export class HospitalController {
  constructor(private repo: HospitalRepository) {}

  criar = async (req: Request, res: Response) => {
    try {
      console.log("Criando novo hospital com dados:", req.body);

      const novo = await this.repo.criar(req.body);
      res.status(201).json(novo);
    } catch (error) {
      console.error("[HospitalController] erro ao criar hospital:", error);
      const msg = error instanceof Error ? error.message : String(error);
      // Em dev, retornar mensagem completa para facilitar debug
      if (process.env.NODE_ENV !== "production") {
        return res
          .status(400)
          .json({ error: "Erro ao criar hospital", details: msg });
      }
      return res.status(400).json({ error: "Erro ao criar hospital" });
    }
  };

  listar = async (_: Request, res: Response) => {
    try {
      console.log("Listando hospitais...");
      const hospitais = await this.repo.buscarTodos();
      console.log("Hospitais encontrados:", hospitais);
      res.json(hospitais);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar hospitais" });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      console.log("Buscando hospital com ID:", id);
      const hospital = await this.repo.buscarPorId(id);
      console.log("Hospital encontrado:", hospital);
      if (!hospital) {
        return res.status(404).json({ error: "Hospital não encontrado" });
      }

      res.json(hospital);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar hospital" });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.atualizar(id, req.body);

      if (!sucesso) {
        return res.status(404).json({ error: "Hospital não encontrado" });
      }

      // Busca o hospital atualizado para retornar
      const hospitalAtualizado = await this.repo.buscarPorId(id);
      res.json(hospitalAtualizado);
    } catch (error) {
      res.status(400).json({ error: "Erro ao atualizar hospital" });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.deletar(id);

      if (!sucesso) {
        return res.status(404).json({ error: "Hospital não encontrado" });
      }

      res.status(204).send(); // 204 No Content para delete bem-sucedido
    } catch (error) {
      res.status(500).json({ error: "Erro ao deletar hospital" });
    }
  };
}
