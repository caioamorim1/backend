import { Request, Response } from "express";
import { ParametrosUnidadeRepository } from "../repositories/parametrosUnidadeRepository";

export class ParametrosUnidadeController {
  constructor(private repo: ParametrosUnidadeRepository) {}

  obter = async (req: Request, res: Response) => {
    const { unidadeId } = req.params;
    const p = await this.repo.obterPorUnidadeId(unidadeId);
    if (!p)
      return res
        .status(404)
        .json({ mensagem: "Parâmetros não encontrados para a unidade" });
    res.json(p);
  };

  create = async (req: Request, res: Response) => {
    const { unidadeId } = req.params;
    console.log("Criando parâmetros para unidadeId:", unidadeId);
    console.log("Dados recebidos:", req.body);
    const salvo = await this.repo.create(unidadeId, req.body);
    res.status(200).json(salvo);
  };

  deletar = async (req: Request, res: Response) => {
    const { unidadeId } = req.params;
    const ok = await this.repo.deletar(unidadeId);
    return ok
      ? res.status(204).send()
      : res.status(404).json({ mensagem: "Nada para remover" });
  };
}
