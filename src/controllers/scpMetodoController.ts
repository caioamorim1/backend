import { Request, Response } from "express";
import { ScpMetodoRepository } from "../repositories/scpMetodoRepository";
import { faixasPorSCP } from "../utils/scpFaixas";
import scpSchemas, { SCPType } from "../utils/scpSchemas";

export class ScpMetodoController {
  constructor(private repo: ScpMetodoRepository) {}

  list = async (_req: Request, res: Response) => {
    const all = await this.repo.list();
    res.json(all);
  };

  get = async (req: Request, res: Response) => {
    const { id } = req.params;
    const found = await this.repo.getById(id);
    if (!found) return res.status(404).json({ error: "Não encontrado" });
    res.json(found);
  };

  getByKey = async (req: Request, res: Response) => {
    const { key } = req.params as { key: string };
    const found = await this.repo.getByKey(key);
    if (!found) return res.status(404).json({ error: "Não encontrado" });
    res.json(found);
  };

  create = async (req: Request, res: Response) => {
    try {
      const body = req.body as any;
      if (!body.key || !body.title || !Array.isArray(body.questions)) {
        return res.status(400).json({
          error: "Payload inválido: key, title e questions são obrigatórios",
        });
      }

      const key = String(body.key).toUpperCase();

      body.key = key;

      // If faixas not provided, attempt to fill from predefined table by SCPType
      let faixas = body.faixas;
      // try to infer SCPType from key if matches known enum
      if (!faixas) {
        const possible = (
          Object.keys(SCPType) as Array<keyof typeof SCPType>
        ).find((k) => k === key);
        if (possible) {
          faixas = (faixasPorSCP as any)[key];
        }
      }

      const dto = { ...body, faixas };
      const created = await this.repo.create(dto);
      res.status(201).json(created);
    } catch (err: any) {
      console.error(err);
      res
        .status(500)
        .json({ error: err.message || "Erro ao criar método SCP" });
    }
  };

  update = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const body = req.body as any;
      if (body.key) body.key = String(body.key).toUpperCase();

      // if key provided and faixas missing, try to fill
      if (body.key && !body.faixas) {
        const possible = (
          Object.keys(SCPType) as Array<keyof typeof SCPType>
        ).find((k) => k === body.key);
        if (possible) body.faixas = (faixasPorSCP as any)[body.key];
      }

      const updated = await this.repo.update(id, body);
      res.json(updated);
    } catch (err: any) {
      console.error(err);
      res
        .status(500)
        .json({ error: err.message || "Erro ao atualizar método SCP" });
    }
  };

  remove = async (req: Request, res: Response) => {
    const { id } = req.params;
    await this.repo.remove(id);
    res.status(204).send();
  };

  seed = async (_req: Request, res: Response) => {
    await this.repo.seedBuiltin();
    res.json({ ok: true });
  };

  // retorna os schemas embutidos (templates) sem criar no banco
  builtin = async (_req: Request, res: Response) => {
    const entries = Object.values(scpSchemas || {}) as any[];
    res.json(
      entries.map((e) => ({
        scp: e.scp,
        title: e.title,
        description: e.description,
        questions: e.questions,
      }))
    );
  };
}
