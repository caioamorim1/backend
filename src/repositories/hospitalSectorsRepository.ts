import { DataSource } from "typeorm";
import {
  InternationSectorDTO,
  AssistanceSectorDTO,
  HospitalSectorsDTO,
} from "../dto/hospitalSectors.dto";

export class HospitalSectorsRepository {
  constructor(private ds: DataSource) {}

  async getAllSectorsByHospital(
    hospitalId: string
  ): Promise<HospitalSectorsDTO> {
    const internation = await this.getInternationSectors(hospitalId);
    const assistance = await this.getAssistanceSectors(hospitalId);

    return {
      id: `hospital-sectors-${hospitalId}`,
      internation,
      assistance,
    };
  }

  private async getInternationSectors(
    hospitalId: string
  ): Promise<InternationSectorDTO[]> {
    const query = `
      SELECT 
        uni.id AS "id",
        uni.nome AS "name",
        uni.descricao AS "descr",
        SUM(
          (
            (COALESCE(REPLACE(c.salario, ',', '.')::numeric, 0) + 
             COALESCE(REPLACE(c.adicionais_tributos, ',', '.')::numeric, 0) +
             COALESCE(REPLACE(uni.horas_extra_reais, ',', '.')::numeric, 0))
            * COALESCE(cuni.quantidade_funcionarios, 0)
          )
        ) AS "costAmount",
        COALESCE(ls.bed_count, 0) AS "bedCount",
        JSON_BUILD_OBJECT(
          'minimumCare',      COALESCE(ls.minimum_care, 0),
          'intermediateCare', COALESCE(ls.intermediate_care, 0),
          'highDependency',   COALESCE(ls.high_dependency, 0),
          'semiIntensive',    COALESCE(ls.semi_intensive, 0),
          'intensive',        COALESCE(ls.intensive, 0)
        ) AS "careLevel",
        JSON_BUILD_OBJECT(
          'evaluated', COALESCE(ls.evaluated, 0),
          'vacant',    COALESCE(ls.vacant, 0),
          'inactive',  COALESCE(ls.inactive, 0)
        ) AS "bedStatus",
        JSON_AGG(
          JSON_BUILD_OBJECT(
            'id', c.id,
            'role', c.nome,
            'quantity', cuni.quantidade_funcionarios
          )
        ) FILTER (WHERE c.id IS NOT NULL) AS "staff"
      FROM public.unidades_internacao uni
      LEFT JOIN public.cargos_unidade cuni ON cuni.unidade_id = uni.id
      LEFT JOIN public.cargo c ON c.id = cuni.cargo_id
      LEFT JOIN public.leitos_status ls ON ls.unidade_id = uni.id
      WHERE uni."hospitalId" = $1
      GROUP BY 
        uni.id, uni.nome, uni.descricao,
        ls.bed_count, ls.minimum_care, ls.intermediate_care,
        ls.high_dependency, ls.semi_intensive, ls.intensive,
        ls.evaluated, ls.vacant, ls.inactive
      ORDER BY uni.nome
    `;

    return await this.ds.query(query, [hospitalId]);
  }

  private async getAssistanceSectors(
    hospitalId: string
  ): Promise<AssistanceSectorDTO[]> {
    const query = `
      SELECT 
        uni.id AS "id",
        uni.nome AS "name",
        uni.descricao AS "descr",
        SUM(
          (
            (COALESCE(REPLACE(c.salario, ',', '.')::numeric, 0) + 
             COALESCE(REPLACE(c.adicionais_tributos, ',', '.')::numeric, 0) +
             COALESCE(REPLACE(uni.horas_extra_reais, ',', '.')::numeric, 0))
            * COALESCE(cuni.quantidade_funcionarios, 0)
          )
        ) AS "costAmount",
        JSON_AGG(
          JSON_BUILD_OBJECT(
            'id', c.id,
            'role', c.nome,
            'quantity', cuni.quantidade_funcionarios
          )
        ) FILTER (WHERE c.id IS NOT NULL) AS "staff"
      FROM public.unidades_nao_internacao uni
      LEFT JOIN public.cargos_unidade cuni ON cuni.unidade_nao_internacao_id = uni.id
      LEFT JOIN public.cargo c ON c.id = cuni.cargo_id
      WHERE uni."hospitalId" = $1
      GROUP BY uni.id, uni.nome, uni.descricao
      ORDER BY uni.nome
    `;

    return await this.ds.query(query, [hospitalId]);
  }
}
