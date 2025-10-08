import { DataSource, Repository } from "typeorm";
import { CargoSitio } from "../entities/CargoSitio";
import { CargoUnidade } from "../entities/CargoUnidade";
import { SitioFuncional } from "../entities/SitioFuncional";
import {
  CreateCargoSitioDTO,
  UpdateCargoSitioDTO,
} from "../dto/cargoSitio.dto";

export class CargoSitioRepository {
  private repo: Repository<CargoSitio>;
  private cargoUnidadeRepo: Repository<CargoUnidade>;
  private sitioRepo: Repository<SitioFuncional>;

  constructor(private ds: DataSource) {
    this.repo = ds.getRepository(CargoSitio);
    this.cargoUnidadeRepo = ds.getRepository(CargoUnidade);
    this.sitioRepo = ds.getRepository(SitioFuncional);
  }

  async listarPorSitio(sitioId: string) {
    return await this.repo.find({
      where: { sitioId },
      relations: ["cargoUnidade", "cargoUnidade.cargo"],
      order: { created_at: "ASC" },
    });
  }

  async criar(data: CreateCargoSitioDTO) {
    // Wrap in transaction and perform pessimistic lock on CargoUnidade to avoid races
    return await this.ds.transaction(async (manager) => {
      const sitio = await manager
        .getRepository(SitioFuncional)
        .findOne({ where: { id: data.sitioId }, relations: ["unidade"] });
      if (!sitio) throw new Error("SitioFuncional não encontrado");

      // Resolve cargoUnidadeId if cargoId provided
      let cargoUnidadeId = (data as any).cargoUnidadeId as string | undefined;
      const cargoId = (data as any).cargoId as string | undefined;

      const cargoUnidadeRepo = manager.getRepository(CargoUnidade);
      const cargoSitioRepo = manager.getRepository(CargoSitio);

      if (!cargoUnidadeId) {
        if (!cargoId)
          throw new Error("cargoUnidadeId ou cargoId é obrigatório");
        const cuFound = await cargoUnidadeRepo.findOne({
          where: {
            cargoId: cargoId,
            unidadeNaoInternacaoId: sitio.unidade?.id,
          },
        });
        if (!cuFound)
          throw new Error(
            "CargoUnidade não encontrado para o cargo informado na unidade do sítio"
          );
        cargoUnidadeId = cuFound.id;
      }

      // Lock the CargoUnidade row for update to avoid concurrent oversubscription
      const cuLocked = await manager
        .createQueryBuilder(CargoUnidade, "cu")
        .setLock("pessimistic_write")
        .where("cu.id = :id", { id: cargoUnidadeId })
        .getOne();

      if (!cuLocked) throw new Error("CargoUnidade não encontrado");

      // Sum existing allocations for this cargoUnidade
      const raw = await cargoSitioRepo
        .createQueryBuilder("cs")
        .select("COALESCE(SUM(cs.quantidade_funcionarios),0)", "sum")
        .where("cs.cargo_unidade_id = :id", { id: cargoUnidadeId })
        .getRawOne();

      const allocated = Number(raw?.sum ?? 0);
      const requested = Number(data.quantidade_funcionarios ?? 1);

      if (allocated + requested > (cuLocked.quantidade_funcionarios ?? 0)) {
        throw new Error(
          "Quantidade solicitada excede a disponibilidade do cargo na unidade"
        );
      }

      // verificar duplicidade
      const existing = await cargoSitioRepo.findOne({
        where: { cargoUnidadeId: cargoUnidadeId, sitioId: data.sitioId },
      });
      if (existing)
        throw new Error("Este cargo já está associado a este sítio");

      const entidade = cargoSitioRepo.create({
        cargoUnidadeId: cargoUnidadeId,
        sitioId: data.sitioId,
        quantidade_funcionarios: data.quantidade_funcionarios ?? 1,
      } as any);

      return await cargoSitioRepo.save(entidade);
    });
  }

  async obter(id: string) {
    const cs = await this.repo.findOne({
      where: { id },
      relations: ["cargoUnidade", "cargoUnidade.cargo", "sitio"],
    });
    if (!cs) throw new Error("CargoSitio não encontrado");
    return cs;
  }

  async atualizar(id: string, data: UpdateCargoSitioDTO) {
    // Use transaction and locks to validate availability if quantidade or cargoUnidadeId changes
    return await this.ds.transaction(async (manager) => {
      const cargoSitioRepo = manager.getRepository(CargoSitio);
      const cargoUnidadeRepo = manager.getRepository(CargoUnidade);

      const existente = await cargoSitioRepo.findOne({
        where: { id },
        relations: ["cargoUnidade"],
      });
      if (!existente) throw new Error("CargoSitio não encontrado");

      const newCargoUnidadeId =
        (data as any).cargoUnidadeId ?? existente.cargoUnidadeId;
      const newQuantidade =
        (data as any).quantidade_funcionarios ??
        existente.quantidade_funcionarios;

      // If cargoUnidadeId changed, ensure the target exists
      const cuLocked = await manager
        .createQueryBuilder(CargoUnidade, "cu")
        .setLock("pessimistic_write")
        .where("cu.id = :id", { id: newCargoUnidadeId })
        .getOne();
      if (!cuLocked) throw new Error("CargoUnidade não encontrado");

      // compute allocated excluding current record
      const raw = await cargoSitioRepo
        .createQueryBuilder("cs")
        .select("COALESCE(SUM(cs.quantidade_funcionarios),0)", "sum")
        .where("cs.cargo_unidade_id = :id", { id: newCargoUnidadeId })
        .andWhere("cs.id != :currentId", { currentId: id })
        .getRawOne();

      const allocatedExcluding = Number(raw?.sum ?? 0);
      if (
        allocatedExcluding + Number(newQuantidade) >
        (cuLocked.quantidade_funcionarios ?? 0)
      ) {
        throw new Error(
          "Quantidade solicitada excede a disponibilidade do cargo na unidade"
        );
      }

      Object.assign(existente, data as any);
      return await cargoSitioRepo.save(existente);
    });
  }

  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }
}
