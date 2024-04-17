package com.bismark.unit;

import com.bismark.test.com.bismark.Student;
import com.bismark.test.com.bismark.StudentRepository;
import com.bismark.test.com.bismark.StudentService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
public class StudentServiceTest {

    @InjectMocks
    private StudentService studentService;

    @Mock
    private StudentRepository studentRepository;

    @Test
    public void testAddStudent() {
        Student newStudent = new Student();
        newStudent.setName("assah");
        newStudent.setEmail("assah@yahoo.com");

                when(studentRepository.save(newStudent)).thenReturn(newStudent);

        Student actualStudent = studentService.addStudent(newStudent);

        Assertions.assertEquals(newStudent, actualStudent);


        verify(studentRepository).save(newStudent);
    }

    @Test
    public void testDeleteStudent(){
        //Arrange
        long studentId = 1L;
        Student expectedStudent = new Student();
        expectedStudent.setName("assah");
        expectedStudent.setEmail("assah@yahoo.com");

        doNothing().when(studentRepository).deleteById(studentId);
        //Act
        studentService.deleteStudent(studentId);

        //check on the StudentRepository.deleteById()

        verify(studentRepository).deleteById(studentId);

    }

    @Test
    public void getStudentById(){
        long studentId = 1L;
        Student expectedStudent = new Student();
        expectedStudent.setName("assah");
        expectedStudent.setEmail("assah@yahoo.com");
        when(studentRepository.findById(studentId)).thenReturn(Optional.of(expectedStudent));

        Student actualStudent = studentService.findStudentById(studentId);

        Assertions.assertEquals(expectedStudent , actualStudent);

        verify(studentRepository).findById(studentId);

    }
}
