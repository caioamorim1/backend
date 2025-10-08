import {
  Column,
  CreateDateColumn,
  Entity,
  OneToMany,
  ManyToOne,
  JoinColumn,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";
import { Colaborador } from "./Colaborador";
import { Hospital } from "./Hospital";
import { CargoUnidade } from "./CargoUnidade";

@Entity("cargo")
export class Cargo {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "varchar", length: 255 })
  nome!: string;

  @Column({ name: "hospital_id" })
  hospitalId!: string;

  @ManyToOne(() => Hospital, (hospital) => hospital.cargos, {
    onDelete: "CASCADE",
    nullable: false,
  })
  @JoinColumn({ name: "hospital_id" })
  hospital!: Hospital;

  @Column({ type: "varchar", length: 255, nullable: true })
  salario?: string;

  @Column({ type: "varchar", length: 255 })
  carga_horaria!: string;

  @Column({ type: "varchar", length: 255, nullable: true })
  adicionais_tributos?: string;

  @Column({ type: "text" })
  descricao!: string;

  @OneToMany(() => CargoUnidade, (cargoUnidade) => cargoUnidade.cargo)
  cargosUnidade!: CargoUnidade[];

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
