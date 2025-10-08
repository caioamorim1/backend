import { Request, Response } from "express";
import { SitioFuncionalRepository } from "../repositories/sitioFuncionalRepository";
import {
  CriarSitioFuncionalDTO,
  AtualizarSitioFuncionalDTO,
  CriarSitioFuncionalDTOWithCargos,
  AtualizarSitioFuncionalDTOWithCargos,
} from "../dto/sitioFuncional.dto";

export class SitioFuncionalController {
  constructor(private repo: SitioFuncionalRepository) {}

  // Listar todos os sítios funcionais
  listar = async (req: Request, res: Response) => {
    try {
      const data = await this.repo.listar();
      return res.json({ data });
    } catch (error) {
      console.error("Erro ao listar sítios funcionais:", error);
      return res.status(500).json({
        message: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Obter um sítio específico
  obter = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sitio = await this.repo.obter(id);

      if (!sitio) {
        return res
          .status(404)
          .json({ message: "Sítio funcional não encontrado" });
      }

      return res.json(sitio);
    } catch (error) {
      console.error("Erro ao obter sítio funcional:", error);
      return res.status(500).json({
        message: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Listar sítios por unidade
  listarPorUnidade = async (req: Request, res: Response) => {
    try {
      const { id: unidadeId } = req.params;
      const data = await this.repo.listarPorUnidade(unidadeId);
      return res.json({ data });
    } catch (error) {
      console.error("Erro ao listar sítios por unidade:", error);
      return res.status(500).json({
        message: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Criar novo sítio funcional
  criar = async (req: Request, res: Response) => {
    try {
      const dados: CriarSitioFuncionalDTOWithCargos = req.body;

      // Validações básicas
      if (!dados.unidadeId) {
        return res.status(400).json({
          message: "unidadeId é obrigatório",
        });
      }

      if (dados.numeroPositions && dados.numeroPositions < 1) {
        return res.status(400).json({
          message: "numeroPositions deve ser pelo menos 1",
        });
      }

      const created = await this.repo.criar(dados);

      if (!created) {
        return res.status(404).json({ message: "Unidade não encontrada" });
      }

      // Garantir que retornamos o sítio já com cargos embutidos
      const sitioComCargos = await this.repo.obter((created as any).id);
      return res.status(201).json(sitioComCargos);
    } catch (error) {
      console.error("Erro ao criar sítio funcional:", error);

      if (
        error instanceof Error &&
        error.message.includes("Formulário de coleta não encontrado")
      ) {
        return res.status(400).json({ message: error.message });
      }

      return res.status(500).json({
        message: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Atualizar sítio funcional
  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const dados: AtualizarSitioFuncionalDTOWithCargos = req.body;

      if (dados.numeroPositions && dados.numeroPositions < 1) {
        return res.status(400).json({
          message: "numeroPositions deve ser pelo menos 1",
        });
      }

      const updated = await this.repo.atualizar(id, dados);

      if (!updated) {
        return res
          .status(404)
          .json({ message: "Sítio funcional não encontrado" });
      }

      // Garantir que retornamos o sítio já com cargos embutidos
      const sitioComCargos = await this.repo.obter(id);
      return res.json(sitioComCargos);
    } catch (error) {
      console.error("Erro ao atualizar sítio funcional:", error);

      if (
        error instanceof Error &&
        error.message.includes("Formulário de coleta não encontrado")
      ) {
        return res.status(400).json({ message: error.message });
      }

      return res.status(500).json({
        message: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Deletar sítio funcional
  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const ok = await this.repo.deletar(id);

      return ok
        ? res.status(204).send()
        : res.status(404).json({ message: "Sítio funcional não encontrado" });
    } catch (error) {
      console.error("Erro ao deletar sítio funcional:", error);
      return res.status(500).json({
        message: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Listar apenas as posições de um sítio
  posicoes = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const sitio = await this.repo.obter(id);
      if (!sitio)
        return res
          .status(404)
          .json({ message: "Sítio funcional não encontrado" });
      // `posicoes` pode não estar tipado na entidade; acessar via any para evitar erro de compilação
      return res.json({ posicoes: (sitio as any).posicoes ?? [] });
    } catch (error) {
      console.error("Erro ao listar posições do sítio:", error);
      return res.status(500).json({ message: "Erro interno do servidor" });
    }
  };
}
