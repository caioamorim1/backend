import { DataSource, Repository } from "typeorm";
import { ScpMetodo } from "../entities/ScpMetodo";
import { CreateScpMetodoDTO, UpdateScpMetodoDTO } from "../dto/scpMetodo.dto";
import { ClassificacaoCuidado } from "../entities/AvaliacaoSCP";
import scpSchemas from "../utils/scpSchemas";
import { faixasPorSCP } from "../utils/scpFaixas";
import { Hospital } from "../entities/Hospital";

export class ScpMetodoRepository {
  private repo: Repository<ScpMetodo>;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(ScpMetodo);
  }

  list() {
    return this.repo.find({
      relations: ["unidades"],
      order: { title: "ASC" },
    });
  }

  getById(id: string) {
    return this.repo.findOne({ where: { id }, relations: ["unidades"] });
  }

  getByKey(key: string) {
    return this.repo.findOne({
      where: { key: key.toUpperCase() },
      relations: ["unidades"],
    });
  }

  async create(dto: CreateScpMetodoDTO) {
    const entity = this.repo.create({
      key: dto.key.toUpperCase(),
      title: dto.title,
      description: dto.description || null,
      questions: dto.questions,
      faixas: dto.faixas,
    });
    return this.repo.save(entity);
  }

  async update(id: string, dto: UpdateScpMetodoDTO) {
    const found = await this.getById(id);
    if (!found) throw new Error("Método SCP não encontrado");

    if (dto.key) found.key = dto.key.toUpperCase();
    if (dto.title !== undefined) found.title = dto.title;
    if (dto.description !== undefined) found.description = dto.description;
    if (dto.questions) found.questions = dto.questions as any;
    if (dto.faixas) found.faixas = dto.faixas as any;

    return this.repo.save(found);
  }

  async remove(id: string) {
    await this.repo.delete({ id });
  }

  // Seed opcional: carrega os métodos embutidos (FUGULIN, PERROCA, DINI)
  async seedBuiltin() {
    const entries = Object.values(scpSchemas) as any[];
    for (const s of entries) {
      const faixas = (faixasPorSCP as any)[s.scp] as Array<{
        min: number;
        max: number;
        classe: ClassificacaoCuidado;
      }>;
      if (!faixas) continue;
      const exists = await this.getByKey(s.scp);
      if (exists) {
        exists.key = s.scp;
        exists.title = s.title;
        exists.description = s.description || null;
        exists.questions = s.questions as any;
        exists.faixas = faixas as any;
        await this.repo.save(exists);
      } else {
        await this.create({
          key: s.scp,
          title: s.title,
          description: s.description,
          questions: s.questions as any,
          faixas: faixas as any,
        });
      }
    }
  }
}
