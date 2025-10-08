import { Request, Response } from "express";
import { BaselineRepository } from "../repositories/baselineRepository";

export class BaselineController {
  constructor(private repo: BaselineRepository) {}

  criar = async (req: Request, res: Response) => {
    try {
      const novo = await this.repo.criar(req.body);
      res.status(201).json(novo);
    } catch (error) {
      res.status(400).json({
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const atualizado = await this.repo.atualizar(req.body, id);
      if (!atualizado)
        return res.status(404).json({ error: "Baseline não encontrado" });
      res.json(atualizado);
    } catch (error) {
      res.status(400).json({
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  listar = async (_: Request, res: Response) => {
    try {
      const items = await this.repo.buscarTodos();
      res.json(items);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar baselines" });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const item = await this.repo.buscarPorId(id);
      console.log("BASELINE", item);
      if (!item)
        return res.status(404).json({ error: "Baseline não encontrado" });
      res.json(item);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar baseline" });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sucesso = await this.repo.deletar(id);
      if (!sucesso)
        return res.status(404).json({ error: "Baseline não encontrado" });
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Erro ao deletar baseline" });
    }
  };

  alterarStatusSetor = async (req: Request, res: Response) => {
    try {
      const { id, setorNome } = req.params;
      const body = req.body ?? {};

      if (!id || !setorNome) {
        return res
          .status(400)
          .json({ error: "id e setorNome são obrigatórios" });
      }

      // se veio ativo no body valida tipo
      let alvoAtivo: boolean | null = null;
      if (Object.prototype.hasOwnProperty.call(body, "ativo")) {
        if (typeof body.ativo !== "boolean") {
          return res
            .status(400)
            .json({ error: "campo 'ativo' deve ser booleano" });
        }
        alvoAtivo = body.ativo;
      }

      // se não veio ativo, faz toggle: busca baseline e inverte o valor atual do setor
      if (alvoAtivo === null) {
        const baseline = await this.repo.buscarPorId(id);
        if (!baseline)
          return res.status(404).json({ error: "Baseline não encontrado" });

        const setores: any[] = (baseline as any).setores ?? [];
        const found = setores.find((s) => {
          const obj =
            typeof s === "string"
              ? (() => {
                  try {
                    return JSON.parse(s);
                  } catch {
                    return { nome: s };
                  }
                })()
              : s;
          return obj && obj.nome === setorNome;
        });
        if (!found)
          return res.status(404).json({ error: "Setor não encontrado" });

        // extrai valor atual e inverte (assume true se ausente)
        const atual = (
          typeof found === "string"
            ? (() => {
                try {
                  return JSON.parse(found);
                } catch {
                  return { ativo: true };
                }
              })()
            : found
        ).ativo;
        alvoAtivo = !Boolean(atual);
      }

      const atualizado = await this.repo.setStatus(id, setorNome, alvoAtivo);
      if (!atualizado)
        return res
          .status(404)
          .json({ error: "Baseline ou setor não encontrado" });

      return res.json(atualizado);
    } catch (err) {
      return res.status(500).json({
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };
}
