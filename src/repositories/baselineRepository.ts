import { DataSource, Repository } from "typeorm";
import { Baseline } from "../entities/Baseline";
import { AtualizarBaselineDTO, CriarBaselineDTO } from "../dto/baseline.dto";
import { Hospital } from "../entities/Hospital";
interface SetorBaseline {
  nome: string;
  custo: string;
  ativo: boolean;
}
export class BaselineRepository {
  private repo: Repository<Baseline>;
  constructor(private ds: DataSource) {
    this.repo = ds.getRepository(Baseline);
  }

  async criar(data: CriarBaselineDTO) {
    return this.ds.transaction(async (manager) => {
      const hospitalRepo = manager.getRepository(Hospital);
      const hospital = await hospitalRepo.findOne({
        where: { id: data.hospitalId },
      });
      if (!hospital) throw new Error("Hospital não encontrado");

      const baselineRepo = manager.getRepository(Baseline);

      const baseline = baselineRepo.create({
        hospitalId: hospital.id,
        nome: data.nome,
        quantidade_funcionarios: data.quantidade_funcionarios,
        custo_total: data.custo_total,
        setores: data.setores as SetorBaseline[],
      } as CriarBaselineDTO);

      await baselineRepo.save(baseline);

      return this.buscarPorId(baseline.id);
    });
  }

  async atualizar(data: AtualizarBaselineDTO, id: string) {
    await this.repo.update(id, data);
    return this.buscarPorId(id);
  }

  buscarTodos() {
    return this.repo.find({
      relations: ["hospital"],
    });
  }

  buscarPorId(id: string) {
    return this.repo.findOne({
      where: { id },
      relations: ["hospital"],
    });
  }
  async setStatus(id: string, setorNome: string, ativo: boolean) {
    // buscar registro atual
    const baseline = await this.buscarPorId(id);
    if (!baseline) return null;

    const setoresOrig = (baseline as any).setores ?? [];
    let encontrou = false;

    const setoresAtualizados = setoresOrig.map((s: any) => {
      // lidar com elementos que podem ser string (JSON) ou objeto
      let obj = s;
      let wasString = false;
      if (typeof s === "string") {
        try {
          obj = JSON.parse(s);
          wasString = true;
        } catch {
          // se não for JSON, tratar como nome simples
          obj = { nome: s, ativo: true };
        }
      }
      if (obj && obj.nome === setorNome) {
        obj.ativo = ativo;
        encontrou = true;
      }
      return wasString ? JSON.stringify(obj) : obj;
    });

    if (!encontrou) return null; // setor não encontrado

    const result = await this.repo.update(
      { id },
      { setores: setoresAtualizados as any }
    );
    if (result.affected && result.affected > 0) {
      return this.buscarPorId(id);
    }
    return null;
  }
  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }
}
