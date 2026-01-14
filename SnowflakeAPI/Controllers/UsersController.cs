using Microsoft.AspNetCore.Mvc;
using SnowflakeAPI.Models;
using SnowflakeAPI.Services;

namespace SnowflakeAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UsersController : ControllerBase
    {
        private readonly SnowflakeService _snowflakeService;
        private readonly ILogger<UsersController> _logger;

        public UsersController(SnowflakeService snowflakeService, ILogger<UsersController> logger)
        {
            _snowflakeService = snowflakeService;
            _logger = logger;
        }

        [HttpGet]
        public async Task<ActionResult<List<User>>> GetUsers()
        {
            try
            {
                var users = await _snowflakeService.GetUsersAsync();
                return Ok(users);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error fetching users");
                return StatusCode(500, new { error = ex.Message, details = ex.ToString() });
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<User>> GetUser(int id)
        {
            try
            {
                var user = await _snowflakeService.GetUserByIdAsync(id);
                if (user == null)
                    return NotFound();
                return Ok(user);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error fetching user {UserId}", id);
                return StatusCode(500, new { error = ex.Message, details = ex.ToString() });
            }
        }

        [HttpPost]
        public async Task<ActionResult> CreateUser([FromBody] User user)
        {
            try
            {
                _logger.LogInformation("Creating user: {Name}, {Email}", user.Name, user.Email);
                await _snowflakeService.CreateUserAsync(user);
                return Ok(new { message = "User created successfully" });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating user");
                return StatusCode(500, new { error = ex.Message, details = ex.ToString() });
            }
        }
[HttpPut("{id}")]
public async Task<ActionResult> UpdateUser(int id, [FromBody] User user)
{
    try
    {
        _logger.LogInformation("Updating user {UserId}: {Name}, {Email}", id, user.Name, user.Email);
        var success = await _snowflakeService.UpdateUserAsync(id, user);
        
        if (!success)
            return NotFound(new { message = "User not found" });
            
        return Ok(new { message = "User updated successfully" });
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error updating user {UserId}", id);
        return StatusCode(500, new { error = ex.Message, details = ex.ToString() });
    }
}
        [HttpDelete("{id}")]
public async Task<ActionResult> DeleteUser(int id)
{
    try
    {
        _logger.LogInformation("Deleting user {UserId}", id);
        var success = await _snowflakeService.DeleteUserAsync(id);
        
        if (!success)
            return NotFound(new { message = "User not found" });
            
        return Ok(new { message = "User deleted successfully" });
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error deleting user {UserId}", id);
        return StatusCode(500, new { error = ex.Message, details = ex.ToString() });
    }
}
    }
}