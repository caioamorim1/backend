import { Request, Response } from "express";
import { QuestionarioRepository } from "../repositories/questionarioRepository";
import {
  CreateQuestionarioDTO,
  UpdateQuestionarioDTO,
  ListQuestionarioDTO,
  QuestionarioResponseDTO,
} from "../dto/questionario.dto";

export class QuestionarioController {
  constructor(private repo: QuestionarioRepository) {}

  criar = async (req: Request, res: Response) => {
    try {
      const { nome, perguntas } = req.body as CreateQuestionarioDTO;

      if (!nome || !perguntas) {
        return res
          .status(400)
          .json({ error: "Nome e perguntas são obrigatórios." });
      }

      const jaExiste = await this.repo.buscarPorNome(nome);
      if (jaExiste) {
        return res
          .status(400)
          .json({ error: "Já existe um questionário com esse nome." });
      }

      const novo = await this.repo.criar({ nome, perguntas });
      return res.status(201).json(novo as QuestionarioResponseDTO);
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: "Erro ao criar questionário." });
    }
  };

  listarTodos = async (req: Request, res: Response) => {
    try {
      const { nome, page, limit } = req.query as unknown as ListQuestionarioDTO;

      const result = await this.repo.listarTodos({
        nome,
        page: page ? Number(page) : 1,
        limit: limit ? Number(limit) : 10,
      });

      return res.json({
        questionarios: result.questionarios as QuestionarioResponseDTO[],
        total: result.total,
      });
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: "Erro ao listar questionários." });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const questionario = await this.repo.buscarPorId(id);

      if (!questionario) {
        return res.status(404).json({ error: "Questionário não encontrado." });
      }

      return res.json(questionario as QuestionarioResponseDTO);
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: "Erro ao buscar questionário." });
    }
  };

  atualizar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const dados = req.body as UpdateQuestionarioDTO;

      const atualizado = await this.repo.atualizar(id, dados);

      if (!atualizado) {
        return res.status(404).json({ error: "Questionário não encontrado." });
      }

      return res.json(atualizado as QuestionarioResponseDTO);
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: "Erro ao atualizar questionário." });
    }
  };

  excluir = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;

      const excluido = await this.repo.excluir(id);

      if (!excluido) {
        return res.status(404).json({ error: "Questionário não encontrado." });
      }

      return res.json({ message: "Questionário excluído com sucesso." });
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: "Erro ao excluir questionário." });
    }
  };
}
