import { Request, Response } from "express";
import { ParametrosNaoInternacaoRepository } from "../repositories/parametrosNaoInternacaoRepository";

export class ParametrosNaoInternacaoController {
  constructor(private repo: ParametrosNaoInternacaoRepository) {}

  obter = async (req: Request, res: Response) => {
    const { unidadeId } = req.params;
    const parametros = await this.repo.obterPorUnidadeId(unidadeId);
    if (!parametros) {
      return res
        .status(404)
        .json({ mensagem: "Parâmetros não encontrados para a unidade" });
    }
    return res.json(parametros);
  };

  salvar = async (req: Request, res: Response) => {
    const { unidadeId } = req.params;
    const body = req.body;
    const salvo = await this.repo.upsert(unidadeId, body);
    return res.status(200).json(salvo);
  };

  deletar = async (req: Request, res: Response) => {
    const { unidadeId } = req.params;
    const removido = await this.repo.deletar(unidadeId);
    if (!removido) {
      return res
        .status(404)
        .json({ mensagem: "Não existem parâmetros cadastrados" });
    }
    return res.status(204).send();
  };
}
