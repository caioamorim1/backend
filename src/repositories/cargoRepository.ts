import { DataSource, Repository } from "typeorm";
import { Cargo } from "../entities/Cargo";
import { Hospital } from "../entities/Hospital";
import { CreateCargoDTO, UpdateCargoDTO } from "../dto/cargo.dto";

export class CargoRepository {
  private repo: Repository<Cargo>;
  private hospitalRepo: Repository<Hospital>;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(Cargo);
    this.hospitalRepo = ds.getRepository(Hospital);
  }

  async criar(data: CreateCargoDTO) {
    // Verificar se o hospital existe
    const hospital = await this.hospitalRepo.findOneBy({ id: data.hospitalId });
    if (!hospital) {
      throw new Error("Hospital não encontrado");
    }

    const ent = this.repo.create(data as any);
    return this.repo.save(ent);
  }

  listar() {
    return this.repo.find({
      relations: ["hospital"],
      order: { nome: "ASC" },
    });
  }

  listarPorHospital(hospitalId: string) {
    return this.repo.find({
      where: { hospitalId },
      relations: ["hospital"],
      order: { nome: "ASC" },
    });
  }

  async obter(id: string) {
    const c = await this.repo.findOne({
      where: { id },
      relations: ["hospital"],
    });
    if (!c) throw new Error("Cargo não encontrado");
    return c;
  }

  async obterPorHospital(cargoId: string, hospitalId: string) {
    const c = await this.repo.findOne({
      where: { id: cargoId, hospitalId },
      relations: ["hospital"],
    });
    if (!c) throw new Error("Cargo não encontrado neste hospital");
    return c;
  }

  async atualizar(id: string, data: UpdateCargoDTO) {
    await this.repo.update(id, data as any);
    return this.obter(id);
  }

  async atualizarPorHospital(
    cargoId: string,
    hospitalId: string,
    data: UpdateCargoDTO
  ) {
    // Verificar se o cargo pertence ao hospital
    const cargo = await this.obterPorHospital(cargoId, hospitalId);

    await this.repo.update(cargoId, data as any);
    return this.obterPorHospital(cargoId, hospitalId);
  }

  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }

  async deletarPorHospital(cargoId: string, hospitalId: string) {
    // Verificar se o cargo pertence ao hospital
    await this.obterPorHospital(cargoId, hospitalId);

    const r = await this.repo.delete({ id: cargoId, hospitalId });
    return (r.affected ?? 0) > 0;
  }
}
