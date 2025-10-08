import { DataSource, getRepository } from "typeorm";

import { Colaborador } from "../entities/Colaborador";
import { Hospital } from "../entities/Hospital";
import * as bcrypt from "bcrypt";
import * as jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "secreto";

export type AuthResult = {
  token: string;
  nome: string;
  id: string;
  hospital?: { id: string; nome: string };
  role?: string;
  mustChangePassword?: boolean;
};

export class AuthService {
  private colaboradorRepo = this.ds.getRepository(Colaborador);

  constructor(private ds: DataSource) {}

  /**
   * Unified login: tries admin by email first, then collaborator by email.
   * Returns AuthResult with token + nome + hospital + cargo + role.
   */
  async login(email: string, senha: string): Promise<AuthResult | null> {
    // try collaborator
    const user = (await this.colaboradorRepo.findOne({
      where: { email },
      relations: ["hospital"],
    })) as Colaborador;
    console.log("USER : ", user);
    if (!user) return null;
    if (!user.senha) return null;
    const okCol = await bcrypt.compare(senha, user.senha as string);
    console.log("OK COL", okCol);
    if (!okCol) return null;

    console.log("COL PARA AUTH", user.hospital);

    const token = jwt.sign(
      {
        id: user.id,
        email: user.email,
        nome: user.nome,
        role: user.permissao,
        mustChangePassword: user.mustChangePassword,
        hospital: user.hospital
          ? { id: user.hospital.id, nome: user.hospital.nome }
          : undefined,
      },
      JWT_SECRET,
      { expiresIn: "8h" }
    );

    return {
      token,
      nome: user.nome,
      id: user.id,
      hospital: user.hospital
        ? { id: user.hospital.id, nome: user.hospital.nome }
        : undefined,
      role: user.permissao,
      mustChangePassword: user.mustChangePassword,
    };
  }

  // Keep a simple logout verifier that just checks token validity (stateless)
  async verifyToken(token: string): Promise<boolean> {
    try {
      jwt.verify(token, JWT_SECRET);
      return true;
    } catch {
      return false;
    }
  }
}
