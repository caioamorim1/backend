import { Request, Response } from "express";
import { LeitoRepository } from "../repositories/leitoRepository";
import { LeitosStatusService } from "../services/leitosStatusService";
import { DataSource } from "typeorm";
import { Leito } from "../entities/Leito";

export class LeitoController {
  private leitosStatusService: LeitosStatusService;

  constructor(private repo: LeitoRepository, private ds: DataSource) {
    this.leitosStatusService = new LeitosStatusService(ds);
  }

  criar = async (req: Request, res: Response) => {
    try {
      const novo = await this.repo.criar(req.body);

      // Atualiza leitos_status após criar novo leito
      if (novo?.unidade?.id) {
        this.leitosStatusService
          .atualizarStatusUnidade(novo.unidade.id)
          .catch((err) => {
            console.warn(
              "Erro ao atualizar leitos_status após criar leito:",
              err
            );
          });
      }

      res.status(201).json(novo);
    } catch (err) {
      console.error("Erro ao criar leito:", err);
      res.status(500).json({
        mensagem: "Erro ao criar leito",
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  listar = async (req: Request, res: Response) => {
    const { unidadeId } = req.query as { unidadeId?: string };
    res.json(await this.repo.listar(unidadeId));
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const id = req.params.id;

      // Busca o leito antes de deletar para pegar a unidade
      const leitoRepo = this.ds.getRepository(Leito);
      const leito = await leitoRepo.findOne({
        where: { id },
        relations: ["unidade"],
      });
      const unidadeId = leito?.unidade?.id;

      const ok = await this.repo.deletar(id);

      if (!ok) {
        return res.status(404).json({ mensagem: "Leito não encontrado" });
      }

      // Atualiza leitos_status após deletar leito
      if (unidadeId) {
        this.leitosStatusService
          .atualizarStatusUnidade(unidadeId)
          .catch((err) => {
            console.warn(
              "Erro ao atualizar leitos_status após deletar leito:",
              err
            );
          });
      }

      return res.status(204).send();
    } catch (err) {
      console.error("Erro ao deletar leito:", err);
      return res.status(500).json({
        mensagem: "Erro ao deletar leito",
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const id = req.params.id;
      const updated = await this.repo.atualizar(id, req.body);

      if (!updated) {
        return res.status(404).json({ mensagem: "Leito não encontrado" });
      }

      // Atualiza leitos_status após atualizar leito
      if (updated?.unidade?.id) {
        this.leitosStatusService
          .atualizarStatusUnidade(updated.unidade.id)
          .catch((err) => {
            console.warn(
              "Erro ao atualizar leitos_status após atualizar leito:",
              err
            );
          });
      }

      return res.json(updated);
    } catch (err) {
      console.error("Erro ao atualizar leito:", err);
      return res.status(500).json({
        mensagem: "Erro ao atualizar leito",
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  atualizarStatus = async (req: Request, res: Response) => {
    const id = req.params.id;
    const { status, justificativa } = req.body as {
      status: string;
      justificativa?: string | null;
    };
    try {
      const updated = await this.repo.atualizarStatus(
        id,
        status,
        justificativa
      );
      if (!updated)
        return res.status(404).json({ mensagem: "Leito não encontrado" });
      return res.json(updated);
    } catch (err: any) {
      // Leito ativo é um erro de negócio tratado como 400
      if (err && (err as any).code === "LEITO_ATIVO") {
        return res.status(400).json({ mensagem: err.message });
      }
      // Erro inesperado
      console.error("Erro em atualizarStatus controller:", err?.message ?? err);
      return res
        .status(500)
        .json({ mensagem: "Erro ao atualizar status do leito" });
    }
  };
}
