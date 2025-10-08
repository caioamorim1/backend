import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { Cargo } from "./Cargo";
import { UnidadeInternacao } from "./UnidadeInternacao";
import { UnidadeNaoInternacao } from "./UnidadeNaoInternacao";

@Entity("cargos_unidade")
export class CargoUnidade {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ name: "cargo_id" })
  cargoId!: string;

  @Column({ name: "unidade_id", nullable: true })
  unidadeId?: string;

  @Column({ name: "unidade_nao_internacao_id", nullable: true })
  unidadeNaoInternacaoId?: string;

  @Column({ type: "int" })
  quantidade_funcionarios!: number;

  @ManyToOne(() => Cargo, (cargo) => cargo.cargosUnidade)
  @JoinColumn({ name: "cargo_id" })
  cargo!: Cargo;

  @ManyToOne(() => UnidadeInternacao, (unidade) => unidade.cargosUnidade, {
    onDelete: "CASCADE",
    nullable: true,
  })
  @JoinColumn({ name: "unidade_id" })
  unidade?: UnidadeInternacao;

  @ManyToOne(() => UnidadeNaoInternacao, (unidade) => unidade.cargosUnidade, {
    onDelete: "CASCADE",
    nullable: true,
  })
  @JoinColumn({ name: "unidade_nao_internacao_id" })
  unidadeNaoInternacao?: UnidadeNaoInternacao;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
