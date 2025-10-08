import { Request, Response } from "express";
import { CargoRepository } from "../repositories/cargoRepository";
import { CreateCargoDTO, UpdateCargoDTO } from "../dto/cargo.dto";

export class CargoHospitalController {
  constructor(private repo: CargoRepository) {}

  // POST /hospitais/:hospitalId/cargos
  criar = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.params;
      const data = req.body as CreateCargoDTO;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(hospitalId)) {
        return res.status(400).json({ mensagem: "ID do hospital inválido" });
      }

      // Validações
      if (!data.nome) {
        return res.status(400).json({ mensagem: "Nome é obrigatório" });
      }

      // Adicionar hospitalId ao payload
      const cargoData = { ...data, hospitalId };

      const created = await this.repo.criar(cargoData);
      res.status(201).json(created);
    } catch (error: any) {
      if (error.message === "Hospital não encontrado") {
        return res.status(404).json({ mensagem: error.message });
      }

      res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error.message,
      });
    }
  };

  // GET /hospitais/:hospitalId/cargos
  listar = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.params;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(hospitalId)) {
        return res.status(400).json({ mensagem: "ID do hospital inválido" });
      }

      const items = await this.repo.listarPorHospital(hospitalId);
      res.json(items);
    } catch (error: any) {
      console.error("Erro ao listar cargos:", error);
      res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error.message,
      });
    }
  };

  // GET /hospitais/:hospitalId/cargos/:cargoId
  obter = async (req: Request, res: Response) => {
    try {
      const { hospitalId, cargoId } = req.params;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(hospitalId) || !uuidRegex.test(cargoId)) {
        return res.status(400).json({ mensagem: "IDs inválidos" });
      }

      const cargo = await this.repo.obterPorHospital(cargoId, hospitalId);
      res.json(cargo);
    } catch (error: any) {
      if (error.message === "Cargo não encontrado neste hospital") {
        return res.status(404).json({ mensagem: error.message });
      }
      console.error("Erro ao obter cargo:", error);
      res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error.message,
      });
    }
  };

  // PATCH /hospitais/:hospitalId/cargos/:cargoId
  atualizar = async (req: Request, res: Response) => {
    try {
      const { hospitalId, cargoId } = req.params;
      const data = req.body as UpdateCargoDTO;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(hospitalId) || !uuidRegex.test(cargoId)) {
        return res.status(400).json({ mensagem: "IDs inválidos" });
      }

      const updated = await this.repo.atualizarPorHospital(
        cargoId,
        hospitalId,
        data
      );
      res.json(updated);
    } catch (error: any) {
      if (error.message === "Cargo não encontrado neste hospital") {
        return res.status(404).json({ mensagem: error.message });
      }
      console.error("Erro ao atualizar cargo:", error);
      res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error.message,
      });
    }
  };

  // DELETE /hospitais/:hospitalId/cargos/:cargoId
  deletar = async (req: Request, res: Response) => {
    try {
      const { hospitalId, cargoId } = req.params;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(hospitalId) || !uuidRegex.test(cargoId)) {
        return res.status(400).json({ mensagem: "IDs inválidos" });
      }

      const ok = await this.repo.deletarPorHospital(cargoId, hospitalId);
      return ok
        ? res.status(204).send()
        : res.status(404).json({ mensagem: "Cargo não encontrado" });
    } catch (error: any) {
      if (error.message === "Cargo não encontrado neste hospital") {
        return res.status(404).json({ mensagem: error.message });
      }
      console.error("Erro ao deletar cargo:", error);
      res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error.message,
      });
    }
  };
}
