import { Request, Response } from "express";
import { UnidadeNaoInternacaoRepository } from "../repositories/unidadeNaoInternacaoRepository";
import { DataSource } from "typeorm";

export class UnidadeNaoInternacaoController {
  constructor(
    private repo: UnidadeNaoInternacaoRepository,
    private ds: DataSource
  ) {}

  criar = async (req: Request, res: Response) => {
    try {
      const dados = req.body;

      // Validações básicas
      if (!dados.hospitalId || !dados.nome) {
        return res.status(400).json({
          mensagem: "hospitalId, nome e tipo são obrigatórios",
        });
      }

      const unidade = await this.repo.criar(dados);
      return res.status(201).json(unidade);
    } catch (error) {
      console.error("Erro ao criar unidade de não-internação:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  listar = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.query as { hospitalId?: string };
      const unidades = await this.repo.listar(hospitalId);
      return res.json({ data: unidades });
    } catch (error) {
      console.error("Erro ao listar unidades de não-internação:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  listarPorHospital = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.params;
      console.log("Hospital ID:", hospitalId);
      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(hospitalId)) {
        return res.status(400).json({ mensagem: "ID do hospital inválido" });
      }

      const unidades = await this.repo.listarPorHospital(hospitalId);
      return res.json({ data: unidades });
    } catch (error) {
      console.error("Erro ao listar unidades por hospital:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  obter = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(id)) {
        return res.status(400).json({ mensagem: "ID inválido" });
      }

      const unidade = await this.repo.obter(id);
      if (!unidade) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      return res.json(unidade);
    } catch (error) {
      console.error("Erro ao obter unidade de não-internação:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const dados = req.body;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(id)) {
        return res.status(400).json({ mensagem: "ID inválido" });
      }

      const unidade = await this.repo.atualizar(id, dados);
      if (!unidade) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      return res.json(unidade);
    } catch (error) {
      console.error("Erro ao atualizar unidade de não-internação:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(id)) {
        return res.status(400).json({ mensagem: "ID inválido" });
      }

      const sucesso = await this.repo.deletar(id);
      if (!sucesso) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      return res.status(204).send();
    } catch (error) {
      console.error("Erro ao deletar unidade de não-internação:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  // Endpoint específico para obter estatísticas de uma unidade de não-internação
  estatisticas = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const { data } = req.query as { data?: string };

      // Validação UUID
      const uuidRegex =
        /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
      if (!uuidRegex.test(id)) {
        return res.status(400).json({ mensagem: "ID inválido" });
      }

      const unidade = await this.repo.obter(id);
      if (!unidade) {
        return res.status(404).json({ mensagem: "Unidade não encontrada" });
      }

      // Calcular estatísticas baseadas nas posições (novo sistema)
      const totalSitios = unidade.sitiosFuncionais?.length || 0;

      const estatisticas = {
        unidade: {
          id: unidade.id,
          nome: unidade.nome,
        },
        sitiosFuncionais: {
          total: totalSitios,
        },
        recursos: {
          cargos: unidade.cargosUnidade?.length || 0,
          funcionarios_total:
            unidade.cargosUnidade?.reduce(
              (sum, c) => sum + c.quantidade_funcionarios,
              0
            ) || 0,
        },
      };

      return res.json(estatisticas);
    } catch (error) {
      console.error("Erro ao obter estatísticas da unidade:", error);
      return res.status(500).json({
        mensagem: "Erro interno do servidor",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };
}
