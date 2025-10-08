import { DataSource, Repository } from "typeorm";
import { Colaborador } from "../entities/Colaborador";
import { Hospital } from "../entities/Hospital";
// import { Cargo } from "../entities/Cargo";
import {
  CreateAdminDTO,
  CreateColaboradorDTO,
  UpdateColaboradorDTO,
} from "../dto/colaborador.dto";
import * as bcrypt from "bcrypt";
import { hash } from "crypto";

const JWT_SECRET = process.env.JWT_SECRET || "secreto";

export class ColaboradorRepository {
  private repo: Repository<Colaborador>;
  private hospitalRepo: Repository<Hospital>;
  // private cargoRepo: Repository<Cargo>;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(Colaborador);
    this.hospitalRepo = ds.getRepository(Hospital);
    // this.cargoRepo = ds.getRepository(Cargo);
  }

  criar = async (data: CreateColaboradorDTO) => {
    const hospital = await this.hospitalRepo.findOneByOrFail({
      id: data.hospitalId,
    });

    // CPF único
    const existing = await this.repo.findOne({ where: { cpf: data.cpf } });
    if (existing) {
      throw new Error("CPF já cadastrado");
    }

    const initialPlain = data.cpf;
    const senhaHash = await bcrypt.hash(initialPlain, 10);

    const novo = this.repo.create({
      hospital: hospital,
      nome: data.nome,
      email: data.email,
      cpf: data.cpf,
      senha: senhaHash,
      mustChangePassword: data.senha ? false : true,
      permissao: data.permissao,
    } as any);
    const saved = await this.repo.save(novo);
    // não retornar hash
    const { senha, ...rest } = saved as any;
    return rest;
  };

  /**
   * Troca a senha do colaborador.
   * - se oldPassword for fornecida, valida antes de trocar.
   * - se não for fornecida (ex.: admin forçando primeiro reset), apenas atualiza.
   */
  changePassword = async (id: string, newPassword: string) => {
    const col = await this.repo.findOneByOrFail({ id });

    const hash = await bcrypt.hash(newPassword, 10);
    col.senha = hash;
    col.mustChangePassword = false;
    await this.repo.save(col);
    const { senha, ...rest } = col as any;
    return rest;
  };

  listar = async (hospitalId?: string) => {
    const base = this.repo
      .createQueryBuilder("c")
      .leftJoinAndSelect("c.hospital", "hospital")
      .orderBy("c.nome", "ASC");
    if (hospitalId) base.where("hospital.id = :hid", { hid: hospitalId });
    const rows = await base.getMany();
    return rows.map(({ senha, ...rest }) => rest as any);
  };

  criarAdmin = async (data: CreateAdminDTO) => {
    const { nome, cpf, email, senha } = data;
    if (!senha) throw new Error("Senha é obrigatória para criar admin");
    if (!email) throw new Error("Email é obrigatório para criar admin");
    const hash = await bcrypt.hash(senha, 10);
    data.senha = hash;
    const novo = this.repo.create({
      nome: nome,
      cpf: cpf || undefined,
      email: email,
      senha: senha,
      permissao: "ADMIN",
      mustChangePassword: true,
    });
    await this.repo.save(novo);
    const { senha: _, ...rest } = novo as any;
    return rest;
  };

  deletarAdmin = async (id: string) => {
    const r = await this.repo.delete({ id, permissao: "ADMIN" });
    return (r.affected ?? 0) > 0;
  };

  listarAdmins = async () => {
    const rows = await this.repo.find({
      where: { permissao: "ADMIN" },
    });
    return rows.map(({ senha, ...rest }) => rest as any);
  };

  obter = async (id: string) => {
    const col = await this.repo.findOne({
      where: { id },
      relations: ["hospital", "hospital.unidades"],
    });
    if (!col) throw new Error("Colaborador não encontrado");
    const { senha, ...rest } = col as any;
    return rest;
  };

  atualizar = async (
    id: string,
    data: UpdateColaboradorDTO & { hospitalId?: string }
  ) => {
    // Buscar o colaborador atual para obter o hospitalId
    const colaboradorAtual = await this.repo.findOne({
      where: { id },
      relations: ["hospital"],
    });
    if (!colaboradorAtual) throw new Error("Colaborador não encontrado");

    // Clone dos dados recebidos
    const updateData: Partial<Colaborador> = { ...data } as any;
    if (data.permissao) {
      updateData.permissao = data.permissao;
    }

    if ((data as any).hospitalId) {
      const hospital = await this.hospitalRepo.findOneByOrFail({
        id: (data as any).hospitalId,
      });
      (updateData as any).hospital = hospital;
      delete (updateData as any).hospitalId;
    }

    // Executa update (TypeORM aceita objeto com relação atribuída)
    await this.repo.update(id, updateData as any);

    // Retorna o colaborador atualizado com relação carregada
    const col = await this.repo.findOne({
      where: { id },
      relations: ["hospital"],
    });
    if (!col) throw new Error("Colaborador não encontrado");
    const { senha, ...rest } = col as any;
    return rest;
  };

  deletar = async (id: string) => {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  };
}
