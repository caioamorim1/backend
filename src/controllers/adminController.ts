// controllers/authController.ts
import { Request, Response } from "express";
import { AdminRepository } from "../repositories/adminRepository";
import { AuthService } from "../services/authService";

export class AdminController {
  constructor(
    private adminRepo: AdminRepository,
    private authService?: AuthService
  ) {}

  async login(req: Request, res: Response) {
    const { email, senha } = req.body;

    try {
      if (!this.authService)
        return res
          .status(500)
          .json({ mensagem: "AuthService não configurado" });

      const out = await this.authService.login(email, senha);
      if (!out)
        return res.status(401).json({ mensagem: "Credenciais inválidas" });

      return res.json(out);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      return res
        .status(500)
        .json({ mensagem: "Erro interno", erro: errorMessage });
    }
  }
}
