import { Request, Response } from "express";
import { CargoRepository } from "../repositories/cargoRepository";
import { CreateCargoDTO, UpdateCargoDTO } from "../dto/cargo.dto";

export class CargoController {
  constructor(private repo: CargoRepository) {}

  criar = async (req: Request, res: Response) => {
    const data = req.body as CreateCargoDTO;
    const created = await this.repo.criar(data);
    res.status(201).json(created);
  };

  listar = async (req: Request, res: Response) => {
    const items = await this.repo.listar();
    res.json(items);
  };

  obter = async (req: Request, res: Response) => {
    try {
      const c = await this.repo.obter(req.params.id);
      res.json(c);
    } catch (e: any) {
      res.status(404).json({ mensagem: e.message || "Não encontrado" });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    const data = req.body as UpdateCargoDTO;
    try {
      const updated = await this.repo.atualizar(req.params.id, data);
      res.json(updated);
    } catch (e: any) {
      res.status(400).json({ mensagem: e.message || String(e) });
    }
  };

  deletar = async (req: Request, res: Response) => {
    const ok = await this.repo.deletar(req.params.id);
    return ok
      ? res.status(204).send()
      : res.status(404).json({ mensagem: "Não encontrado" });
  };
}
