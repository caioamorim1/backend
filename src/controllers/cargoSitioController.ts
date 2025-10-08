import { Request, Response } from "express";
import { CargoSitioRepository } from "../repositories/cargoSitioRepository";
import {
  CreateCargoSitioDTO,
  UpdateCargoSitioDTO,
} from "../dto/cargoSitio.dto";

export class CargoSitioController {
  constructor(private repo: CargoSitioRepository) {}

  listarPorSitio = async (req: Request, res: Response) => {
    try {
      const { id: sitioId } = req.params;
      const data = await this.repo.listarPorSitio(sitioId);
      return res.json({ data });
    } catch (error) {
      console.error("Erro ao listar cargos por sitio:", error);
      return res
        .status(500)
        .json({ message: "Erro interno", error: String(error) });
    }
  };

  criar = async (req: Request, res: Response) => {
    try {
      const dados: CreateCargoSitioDTO = req.body;
      const novo = await this.repo.criar(dados);
      return res.status(201).json(novo);
    } catch (error) {
      console.error("Erro ao criar cargo por sitio:", error);
      return res
        .status(400)
        .json({
          message: error instanceof Error ? error.message : String(error),
        });
    }
  };

  obter = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const cs = await this.repo.obter(id);
      return res.json(cs);
    } catch (error) {
      console.error("Erro ao obter cargo por sitio:", error);
      return res
        .status(404)
        .json({
          message: error instanceof Error ? error.message : String(error),
        });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const dados: UpdateCargoSitioDTO = req.body;
      const updated = await this.repo.atualizar(id, dados);
      return res.json(updated);
    } catch (error) {
      console.error("Erro ao atualizar cargo por sitio:", error);
      return res
        .status(400)
        .json({
          message: error instanceof Error ? error.message : String(error),
        });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const ok = await this.repo.deletar(id);
      return ok
        ? res.status(204).send()
        : res.status(404).json({ message: "não encontrado" });
    } catch (error) {
      console.error("Erro ao deletar cargo por sitio:", error);
      return res.status(500).json({ message: "Erro interno" });
    }
  };
}
