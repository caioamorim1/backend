import { DataSource, Repository } from "typeorm";
import { Leito, StatusLeito } from "../entities/Leito";
import { UnidadeInternacao } from "../entities/UnidadeInternacao";
import { CreateLeitoDTO } from "../dto/leito.dto";
import { LeitosStatusService } from "../services/leitosStatusService";

export class LeitoRepository {
  private repo: Repository<Leito>;
  private unidadeRepo: Repository<UnidadeInternacao>;
  private leitosStatusService: LeitosStatusService;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(Leito);
    this.unidadeRepo = ds.getRepository(UnidadeInternacao);
    this.leitosStatusService = new LeitosStatusService(ds);
  }

  async criar(data: CreateLeitoDTO) {
    console.log(data);
    const unidade = await this.unidadeRepo.findOneByOrFail({
      id: data.unidadeId,
    });
    const ent = this.repo.create({ unidade, numero: data.numero });
    return this.repo.save(ent);
  }

  listar(unidadeId?: string) {
    if (unidadeId)
      return this.repo.find({
        where: { unidade: { id: unidadeId } },
        relations: ["unidade"],
      });
    return this.repo.find({ relations: ["unidade"] });
  }

  async buscarPorId(id: string) {
    return this.repo.findOne({
      where: { id },
      relations: ["unidade"],
    });
  }

  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }

  async atualizar(
    id: string,
    data: Partial<{ numero: string; unidadeId: string }>
  ) {
    const ent = await this.repo.findOne({ where: { id } });
    if (!ent) return null;
    if (data.numero !== undefined) ent.numero = data.numero;
    if (data.unidadeId !== undefined) {
      const unidade = await this.unidadeRepo.findOneByOrFail({
        id: data.unidadeId,
      });
      ent.unidade = unidade;
    }
    return this.repo.save(ent);
  }

  async atualizarStatus(
    id: string,
    status: string,
    justificativa?: string | null
  ) {
    // Busca a entidade
    const ent = await this.repo.findOne({
      where: { id },
      relations: ["unidade"],
    });
    if (!ent) return null;

    // Se já estiver ATIVO, não permite alteração (regra de negócio)
    if (ent.status === StatusLeito.ATIVO) {
      const e = new Error("Não é possível alterar o status de um leito ativo");
      // marca o erro para ser tratado pelo controller
      (e as any).code = "LEITO_ATIVO";
      throw e;
    }

    console.log("Status : ", status);

    try {
      // tipo é enum StatusLeito — atribuição direta (validação básica)
      (ent as any).status = status;
      if (justificativa !== undefined) {
        (ent as any).justificativa = justificativa;
      }
      const saved = await this.repo.save(ent);

      // Atualiza leitos_status da unidade após mudar status do leito
      if (ent.unidade?.id) {
        try {
          await this.leitosStatusService.atualizarStatusUnidade(ent.unidade.id);
        } catch (e) {
          console.warn("Não foi possível atualizar leitos_status:", e);
        }
      }

      return saved;
    } catch (err: any) {
      // Loga internamente e repassa uma mensagem amigável
      console.error("Erro ao atualizar status do leito:", err?.message ?? err);
      const e = new Error("Erro ao persistir alteração de status do leito");
      (e as any).cause = err;
      throw e;
    }
  }
}
