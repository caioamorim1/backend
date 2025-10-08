import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { CargoUnidade } from "./CargoUnidade";
import { SitioFuncional } from "./SitioFuncional";

@Entity("cargos_sitio")
export class CargoSitio {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ name: "cargo_unidade_id" })
  cargoUnidadeId!: string;

  @Column({ name: "sitio_id" })
  sitioId!: string;

  @Column({ type: "int", default: 1 })
  quantidade_funcionarios!: number;

  @ManyToOne(() => CargoUnidade, (cu) => cu.id, { nullable: false })
  @JoinColumn({ name: "cargo_unidade_id" })
  cargoUnidade!: CargoUnidade;

  @ManyToOne(() => SitioFuncional, (s) => s.id, {
    nullable: false,
    onDelete: "CASCADE",
  })
  @JoinColumn({ name: "sitio_id" })
  sitio!: SitioFuncional;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
