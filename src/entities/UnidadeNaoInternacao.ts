import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  OneToMany,
} from "typeorm";
import { Hospital } from "./Hospital";
import { SitioFuncional } from "./SitioFuncional";
import { CargoUnidade } from "./CargoUnidade";

// Tipos são agora campos de texto livres, não mais enums fixos

@Entity("unidades_nao_internacao")
export class UnidadeNaoInternacao {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @ManyToOne(() => Hospital, (h) => h.unidadesNaoInternacao, {
    nullable: false,
    onDelete: "CASCADE",
  })
  hospital!: Hospital;

  @Column({ type: "varchar", length: 120 })
  nome!: string;

  @Column({ type: "varchar", length: 20, default: "ativo" })
  status!: string; // 'ativo' | 'inativo'

  @Column({ type: "text", nullable: true })
  descricao?: string;

  @Column({ type: "varchar", length: 255, nullable: true })
  horas_extra_reais?: string;

  @Column({ type: "varchar", length: 255, nullable: true })
  horas_extra_projetadas?: string;

  @OneToMany(() => SitioFuncional, (sitio) => sitio.unidade, {
    cascade: true,
  })
  sitiosFuncionais!: SitioFuncional[];

  @OneToMany(
    () => CargoUnidade,
    (cargoUnidade) => cargoUnidade.unidadeNaoInternacao,
    {
      cascade: true,
    }
  )
  cargosUnidade!: CargoUnidade[];

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
