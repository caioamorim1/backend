import { DataSource, Repository } from "typeorm";
import { Admin } from "../entities/Admin";
import { CreateAdminDTO, LoginAdminDTO } from "../dto/admin.dto";
import * as bcrypt from "bcrypt";
import * as jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "secreto";

export class AdminRepository {
  private repository: Repository<Admin>;

  constructor(private dataSource: DataSource) {
    this.repository = this.dataSource.getRepository(Admin);
  }

  async criar(dados: CreateAdminDTO): Promise<Admin> {
    const senhaHash = await bcrypt.hash(dados.senha, 10);
    const admin = this.repository.create({
      ...dados,
      senha: senhaHash,
    });
    return await this.repository.save(admin);
  }

  async deletar(id: string): Promise<boolean> {
    const resultado = await this.repository.delete(id);
    return (resultado.affected ?? 0) > 0;
  }

  async buscarTodos(): Promise<Admin[]> {
    return await this.repository.find();
  }

  async atualizarAdmin(id: string, dados: CreateAdminDTO): Promise<Admin> {
    await this.repository.update(id, {
      ...dados,
      senha: await bcrypt.hash(dados.senha, 10),
    });
    const admin = await this.repository.findOneBy({ id });
    if (!admin) {
      throw new Error("Admin não encontrado");
    }
    return admin;
  }
}
