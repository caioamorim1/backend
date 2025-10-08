import { Request, Response } from "express";
import { ColaboradorRepository } from "../repositories/colaboradorRepository";
import { AuthService } from "../services/authService";

export class ColaboradorController {
  constructor(
    private repo: ColaboradorRepository,
    private authService?: AuthService
  ) {}

  criar = async (req: Request, res: Response) => {
    const novo = await this.repo.criar(req.body);
    res.status(201).json(novo);
  };

  listar = async (req: Request, res: Response) => {
    const { hospitalId, unidadeId } = req.query as {
      hospitalId?: string;
      unidadeId?: string;
    };
    // Compatibilidade: aceita hospitalId (novo) ou unidadeId (antigo campo)
    const filterId = hospitalId ?? unidadeId;
    res.json(await this.repo.listar(filterId));
  };

  listarAdmins = async (req: Request, res: Response) => {
    const admins = await this.repo.listarAdmins();
    res.json(admins);
  };

  deletarAdmin = async (req: Request, res: Response) => {
    const { id } = req.params;
    const colaborador = await this.repo.obter(id);
    if (!colaborador || colaborador.permissao !== "ADMIN") {
      return res.status(404).json({ mensagem: "Admin não encontrado" });
    }
    const ok = await this.repo.deletarAdmin(id);
    return ok
      ? res.status(204).send()
      : res.status(404).json({ mensagem: "Não encontrado" });
  };

  criarAdmin = async (req: Request, res: Response) => {
    try {
      console.log("Criando admin com dados:", req.body);
      const novoAdmin = await this.repo.criarAdmin(req.body);
      res.status(201).json(novoAdmin);
    } catch (error: any) {
      res.status(400).json({ erro: error.message || String(error) });
    }
  };

  obter = async (req: Request, res: Response) => {
    const col = await this.repo.obter(req.params.id);
    console.log(req.params.id, col);
    res.json(col);
  };

  atualizar = async (req: Request, res: Response) => {
    const up = await this.repo.atualizar(req.params.id, req.body);
    res.json(up);
  };

  deletar = async (req: Request, res: Response) => {
    const ok = await this.repo.deletar(req.params.id);
    return ok
      ? res.status(204).send()
      : res.status(404).json({ mensagem: "Não encontrado" });
  };

  login = async (req: Request, res: Response) => {
    const { email, senha } = req.body as { email: string; senha: string };
    if (!email || !senha)
      return res.status(400).json({ erro: "email e senha são obrigatórios" });
    if (!this.authService)
      return res.status(500).json({ erro: "AuthService não configurado" });
    const out = await this.authService.login(email, senha);
    if (!out) return res.status(401).json({ erro: "Credenciais inválidas" });
    res.json(out);
  };

  changePassword = async (req: Request, res: Response) => {
    const { id } = req.params;
    const { senha } = req.body;
    if (!senha) return res.status(400).json({ erro: "senha é obrigatório" });
    try {
      const updated = await this.repo.changePassword(id, senha);

      if (updated) {
        console.log("Senha alterada com sucesso");
      }
      res.json(updated);
    } catch (e: any) {
      res.status(400).json({ erro: e.message || String(e) });
    }
  };
}
