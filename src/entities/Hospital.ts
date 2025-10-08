import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  OneToMany,
  ManyToOne,
  OneToOne,
} from "typeorm";
import { UnidadeInternacao } from "./UnidadeInternacao";
import { UnidadeNaoInternacao } from "./UnidadeNaoInternacao";
import { Colaborador } from "./Colaborador";
import { Cargo } from "./Cargo";
import { Regiao } from "./Regiao";
import { Baseline } from "./Baseline";

@Entity("hospitais")
export class Hospital {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "varchar", length: 255 })
  nome!: string;

  @Column({ type: "varchar", length: 20, nullable: true, unique: true })
  cnpj!: string;

  @Column({ type: "varchar", length: 255, nullable: true })
  endereco!: string;

  @Column({ type: "varchar", length: 40, nullable: true })
  telefone!: string;

  @OneToMany(() => Colaborador, (c) => c.hospital)
  colaboradores!: Colaborador[];

  @OneToMany(() => Cargo, (cargo) => cargo.hospital)
  cargos!: Cargo[];

  @OneToOne(() => Baseline, (baseline) => baseline.hospital, {
    nullable: true,
    cascade: true,
  })
  baseline!: Baseline;

  @OneToMany(() => UnidadeInternacao, (ui) => ui.hospital)
  unidades!: UnidadeInternacao[];

  @OneToMany(() => UnidadeNaoInternacao, (uni) => uni.hospital)
  unidadesNaoInternacao!: UnidadeNaoInternacao[];

  @ManyToOne(() => Regiao, (r) => r.hospitais, { nullable: true })
  regiao?: Regiao;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
