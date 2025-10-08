import { Request, Response } from "express";
import { AuthService } from "../services/authService";

export class AuthController {
  constructor(private authService: AuthService) {}

  login = async (req: Request, res: Response) => {
    const { email, senha } = req.body as { email?: string; senha?: string };
    if (!email || !senha)
      return res.status(400).json({ error: "email e senha são obrigatórios" });

    try {
      const out = await this.authService.login(email, senha);
      if (!out) return res.status(401).json({ error: "Credenciais inválidas" });
      return res.json(out);
    } catch (err) {
      const details = err instanceof Error ? err.message : String(err);
      return res.status(500).json({ error: "Erro no login", details });
    }
  };
}
